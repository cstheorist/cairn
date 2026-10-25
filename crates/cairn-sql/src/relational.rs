use crate::{
    expression::{self, Compiler, Expression, Field, Row},
    sql::{Engine, Output, Plan},
    storage::{Column, Schema},
    transaction::Session,
    DbError, Result,
};
use rmpv::Value;
use sqlparser::{ast::*, dialect::PostgreSqlDialect, parser::Parser};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::{Arc, LazyLock},
};

const MAX_ROWS: usize = 100_000;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_JOIN_WORK: usize = 5_000_000;
static COMPUTE: LazyLock<Arc<tokio::sync::Semaphore>> = LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(4)));

#[derive(Clone, Debug)]
struct Source {
    schema: Schema,
    start: usize,
    lookup: Option<Expression>,
}
#[derive(Clone, Debug)]
enum JoinKind {
    Cross,
    Inner,
    Left,
    Right,
    Full,
}
#[derive(Clone, Debug)]
struct Join {
    source: usize,
    kind: JoinKind,
    condition: Option<Expression>,
}
#[derive(Clone, Debug)]
struct Order {
    expression: Expression,
    ascending: bool,
    nulls_first: bool,
}

#[derive(Clone, Debug)]
pub struct SelectPlan {
    sources: Vec<Source>,
    joins: Vec<Join>,
    projection: Vec<Expression>,
    filter: Option<Expression>,
    groups: Vec<Expression>,
    having: Option<Expression>,
    order: Vec<Order>,
    limit: Option<Expression>,
    offset: Option<Expression>,
    aggregate: bool,
    distinct: bool,
    width: usize,
}

fn unsupported() -> DbError {
    expression::unsupported()
}
fn quota() -> DbError {
    DbError::new("54000", "query exceeds the configured row, memory, or join-work budget")
}
fn normalize(id: &Ident) -> String {
    if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_ascii_lowercase()
    }
}
fn cost(value: &Value) -> usize {
    std::mem::size_of::<Value>()
        + match value {
            Value::String(value) => value.as_bytes().len(),
            Value::Binary(value) => value.len(),
            Value::Array(values) => values.iter().map(cost).sum(),
            _ => 0,
        }
}
fn row_cost(row: &[Value]) -> usize {
    row.iter().map(cost).sum::<usize>() + std::mem::size_of::<Row>()
}
pub fn rows_cost(rows: &[Row]) -> usize {
    rows.iter().map(|row| row_cost(row)).sum()
}
fn check_rows(rows: &[Row]) -> Result<()> {
    if rows.len() > MAX_ROWS || rows.iter().map(|row| row_cost(row)).sum::<usize>() > MAX_BYTES {
        return Err(quota());
    }
    Ok(())
}
fn boolean(expression: &Expression) -> Result<()> {
    if expression.kind != "boolean" {
        return Err(DbError::new("42804", "predicate must be BOOLEAN"));
    }
    if expression.is_aggregate() {
        return Err(DbError::new("42803", "aggregate is not allowed in this predicate"));
    }
    Ok(())
}
fn label(expression: &Expr) -> String {
    match expression {
        Expr::Identifier(id) => normalize(id),
        Expr::CompoundIdentifier(path) => path.last().map(normalize).unwrap_or_else(|| "?column?".into()),
        Expr::Function(function) => function.name.to_string().to_ascii_lowercase(),
        _ => "?column?".into(),
    }
}

async fn source(
    engine: &Engine,
    factor: &TableFactor,
    fields: &mut Vec<Field>,
    sources: &mut Vec<Source>,
    aliases: &mut HashSet<String>,
) -> Result<()> {
    let TableFactor::Table {
        name,
        alias,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
    } = factor
    else {
        return Err(unsupported());
    };
    if !with_hints.is_empty() || !partitions.is_empty() || alias.as_ref().is_some_and(|alias| !alias.columns.is_empty())
    {
        return Err(unsupported());
    }
    let schema = engine.schema(name).await?;
    let qualifier = alias
        .as_ref()
        .map(|alias| normalize(&alias.name))
        .unwrap_or_else(|| schema.name.clone());
    if !aliases.insert(qualifier.clone()) {
        return Err(DbError::new("42712", "duplicate table alias"));
    }
    let start = fields.len();
    for column in &schema.columns {
        fields.push(Field {
            qualifier: qualifier.clone(),
            column: column.clone(),
        });
    }
    sources.push(Source {
        schema,
        start,
        lookup: None,
    });
    if sources.len() > 16 {
        return Err(quota());
    }
    Ok(())
}

fn clause_expression(
    ast: &Expr,
    compiler: &mut Compiler,
    projection: &[Expression],
    columns: &[Column],
    prefer_alias: bool,
) -> Result<Expression> {
    if let Expr::Value(sqlparser::ast::Value::Number(number, false)) = ast {
        let position = number
            .parse::<usize>()
            .ok()
            .filter(|p| *p > 0 && *p <= projection.len())
            .ok_or_else(|| DbError::new("42P10", "GROUP/ORDER BY position is outside the select list"))?;
        return Ok(projection[position - 1].clone());
    }
    let alias = if let Expr::Identifier(id) = ast {
        let name = normalize(id);
        let matching = columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.name == name)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(DbError::new("42702", "output alias is ambiguous"));
        }
        matching.first().map(|index| projection[*index].clone())
    } else {
        None
    };
    if prefer_alias {
        if let Some(alias) = alias.as_ref() {
            return Ok(alias.clone());
        }
    } else if let Ok(expression) = compiler.compile(ast, None) {
        return Ok(expression);
    }
    if let Some(alias) = alias {
        return Ok(alias);
    }
    compiler.compile(ast, None)
}

pub async fn prepare(engine: &Engine, sql: &str) -> Result<Plan> {
    let statements =
        Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|error| DbError::new("42601", error.to_string()))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err(unsupported());
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(unsupported());
    };
    let distinct = match select.distinct {
        None => false,
        Some(Distinct::Distinct) => true,
        _ => return Err(unsupported()),
    };
    let mut fields = Vec::new();
    let mut sources = Vec::new();
    let mut aliases = HashSet::new();
    let mut pending_joins = Vec::new();
    for table in &select.from {
        source(engine, &table.relation, &mut fields, &mut sources, &mut aliases).await?;
        if sources.len() > 1 {
            pending_joins.push((sources.len() - 1, JoinKind::Cross, None, fields.len()));
        }
        for join in &table.joins {
            if join.global {
                return Err(unsupported());
            }
            source(engine, &join.relation, &mut fields, &mut sources, &mut aliases).await?;
            let (kind, condition) = match &join.join_operator {
                JoinOperator::CrossJoin => (JoinKind::Cross, None),
                JoinOperator::Inner(JoinConstraint::On(expr)) => (JoinKind::Inner, Some(expr.clone())),
                JoinOperator::LeftOuter(JoinConstraint::On(expr)) => (JoinKind::Left, Some(expr.clone())),
                JoinOperator::RightOuter(JoinConstraint::On(expr)) => (JoinKind::Right, Some(expr.clone())),
                JoinOperator::FullOuter(JoinConstraint::On(expr)) => (JoinKind::Full, Some(expr.clone())),
                _ => return Err(unsupported()),
            };
            pending_joins.push((sources.len() - 1, kind, condition, fields.len()));
        }
    }
    let width = fields.len();
    let mut compiler = Compiler::new(fields);
    let mut joins = Vec::new();
    for (source, kind, condition, visible) in pending_joins {
        let all_fields = std::mem::take(&mut compiler.fields);
        compiler.fields = all_fields[..visible].to_vec();
        let condition = condition
            .as_ref()
            .map(|condition| compiler.compile(condition, Some("boolean")))
            .transpose()?;
        if let Some(condition) = &condition {
            boolean(condition)?;
        }
        compiler.fields = all_fields;
        joins.push(Join {
            source,
            kind,
            condition,
        });
    }
    let filter = select
        .selection
        .as_ref()
        .map(|expr| compiler.compile(expr, Some("boolean")))
        .transpose()?;
    if let Some(filter) = &filter {
        boolean(filter)?;
    }
    for source in &mut sources {
        source.lookup = filter
            .as_ref()
            .and_then(|filter| filter.equality_value(source.start + source.schema.primary_key));
    }
    let mut projection = Vec::new();
    let mut columns = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(options) if options.to_string().is_empty() => {
                for (index, field) in compiler.fields.iter().enumerate() {
                    projection.push(Expression::column_reference(index, &field.column));
                    columns.push(field.column.clone());
                }
            }
            SelectItem::QualifiedWildcard(prefix, options) if options.to_string().is_empty() && prefix.0.len() == 1 => {
                let qualifier = normalize(&prefix.0[0]);
                if !aliases.contains(&qualifier) {
                    return Err(DbError::new("42P01", "unknown wildcard qualifier"));
                }
                for (index, field) in compiler
                    .fields
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| field.qualifier == qualifier)
                {
                    projection.push(Expression::column_reference(index, &field.column));
                    columns.push(field.column.clone());
                }
            }
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                let expression = compiler.compile(expr, None)?;
                let name = if let SelectItem::ExprWithAlias { alias, .. } = item {
                    normalize(alias)
                } else {
                    label(expr)
                };
                columns.push(expression.column(name));
                projection.push(expression);
            }
            _ => return Err(unsupported()),
        }
    }
    if projection.is_empty() || projection.len() > 1024 {
        return Err(quota());
    }
    let GroupByExpr::Expressions(group_by, modifiers) = &select.group_by else {
        return Err(unsupported());
    };
    if !modifiers.is_empty() {
        return Err(unsupported());
    }
    let groups = group_by
        .iter()
        .map(|expr| clause_expression(expr, &mut compiler, &projection, &columns, false))
        .collect::<Result<Vec<_>>>()?;
    if groups.iter().any(Expression::is_aggregate) {
        return Err(DbError::new("42803", "GROUP BY cannot contain aggregates"));
    }
    let having = select
        .having
        .as_ref()
        .map(|expr| compiler.compile(expr, Some("boolean")))
        .transpose()?;
    if having.as_ref().is_some_and(|having| having.kind != "boolean") {
        return Err(DbError::new("42804", "HAVING must be BOOLEAN"));
    }
    let mut order = Vec::new();
    if let Some(order_by) = &query.order_by {
        if order_by.interpolate.is_some() {
            return Err(unsupported());
        }
        for item in &order_by.exprs {
            if item.with_fill.is_some() {
                return Err(unsupported());
            }
            let expression = clause_expression(&item.expr, &mut compiler, &projection, &columns, true)?;
            if distinct && !projection.contains(&expression) {
                return Err(DbError::new("42P10", "ORDER BY must appear in a DISTINCT select list"));
            }
            let ascending = item.asc.unwrap_or(true);
            order.push(Order {
                expression,
                ascending,
                nulls_first: item.nulls_first.unwrap_or(!ascending),
            });
        }
    }
    let aggregate = !groups.is_empty()
        || having.is_some()
        || projection.iter().any(Expression::is_aggregate)
        || order.iter().any(|order| order.expression.is_aggregate());
    if aggregate
        && (projection.iter().any(|expr| !expr.valid_group(&groups))
            || having.as_ref().is_some_and(|expr| !expr.valid_group(&groups))
            || order.iter().any(|order| !order.expression.valid_group(&groups)))
    {
        return Err(DbError::new("42803", "column must appear in GROUP BY or an aggregate"));
    }
    let limit = query
        .limit
        .as_ref()
        .map(|expr| compiler.compile(expr, Some("bigint")))
        .transpose()?;
    let offset = query
        .offset
        .as_ref()
        .map(|offset| compiler.compile(&offset.value, Some("bigint")))
        .transpose()?;
    if limit.as_ref().is_some_and(|expr| !expr.is_constant()) || offset.as_ref().is_some_and(|expr| !expr.is_constant())
    {
        return Err(DbError::new("42P10", "LIMIT/OFFSET cannot refer to rows"));
    }
    let mut allowed = format!(
        "SELECT {}{}",
        if distinct { "DISTINCT " } else { "" },
        select
            .projection
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    if !select.from.is_empty() {
        allowed.push_str(&format!(
            " FROM {}",
            select
                .from
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(selection) = &select.selection {
        allowed.push_str(&format!(" WHERE {selection}"));
    }
    if !group_by.is_empty() {
        allowed.push_str(&format!(
            " GROUP BY {}",
            group_by.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
        ));
    }
    if let Some(having) = &select.having {
        allowed.push_str(&format!(" HAVING {having}"));
    }
    if let Some(order) = &query.order_by {
        allowed.push_str(&format!(" {order}"));
    }
    if let Some(limit) = &query.limit {
        allowed.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = &query.offset {
        allowed.push_str(&format!(" {offset}"));
    }
    crate::sql::exact(&statements[0], allowed)?;
    let parameters = compiler
        .parameters
        .into_iter()
        .map(|parameter| parameter.ok_or_else(|| DbError::new("42P02", "parameter numbers must be contiguous")))
        .collect::<Result<Vec<_>>>()?;
    let select = SelectPlan {
        sources,
        joins,
        projection,
        filter,
        groups,
        having,
        order,
        limit,
        offset,
        aggregate,
        distinct,
        width,
    };
    Ok(Plan::select(select, columns, parameters, sql.len()))
}

impl SelectPlan {
    pub(crate) fn point_key(&self) -> Option<&Expression> {
        if self.sources.len() == 1 && self.joins.is_empty() {
            self.filter
                .as_ref()?
                .exact_equality_value(self.sources[0].schema.primary_key)
        } else {
            None
        }
    }
    pub(crate) fn table_rows(schema: Schema, filter: Option<Expression>) -> Self {
        let projection = schema
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| Expression::column_reference(index, column))
            .collect();
        let width = schema.columns.len();
        let lookup = filter
            .as_ref()
            .and_then(|filter| filter.equality_value(schema.primary_key));
        Self {
            sources: vec![Source {
                schema,
                start: 0,
                lookup,
            }],
            joins: vec![],
            projection,
            filter,
            groups: vec![],
            having: None,
            order: vec![],
            limit: None,
            offset: None,
            aggregate: false,
            distinct: false,
            width,
        }
    }
    pub fn needs_transaction(&self) -> bool {
        self.sources.len() > 1 || self.sources.iter().any(|source| source.lookup.is_none())
    }

    pub async fn execute(&self, engine: &Engine, parameters: &[Value], session: &mut Session) -> Result<Output> {
        let mut inputs = Vec::new();
        let mut memory = 0usize;
        for source in &self.sources {
            let mut rows = Vec::new();
            if let Some(lookup) = &source.lookup {
                let key = lookup.eval(&[], parameters, None)?;
                if !key.is_nil() {
                    let key = key
                        .as_i64()
                        .ok_or_else(|| DbError::new("42804", "primary key must be BIGINT"))?;
                    let shard = engine.cluster.owner(key);
                    let args = vec![source.schema.name.as_str().into(), key.into()];
                    let response = if let Some(transaction) = &mut session.transaction {
                        transaction.call(&engine.cluster, shard, "cairn.tx_read", args).await?
                    } else {
                        engine.cluster.read(shard, args).await?
                    };
                    let row = response
                        .first()
                        .and_then(Value::as_array)
                        .ok_or_else(|| DbError::new("XX000", "invalid tuple response"))?;
                    if !row.is_empty() {
                        memory = memory.saturating_add(row_cost(row));
                        if memory > MAX_BYTES {
                            return Err(quota());
                        }
                        rows.push(row.clone());
                    }
                }
            } else {
                let transaction = session
                    .transaction
                    .as_mut()
                    .ok_or_else(|| DbError::new("XX000", "scan requires a transaction"))?;
                for shard in 0..engine.cluster.shard_count() {
                    let mut after = Value::Nil;
                    loop {
                        let response = transaction
                            .call(
                                &engine.cluster,
                                shard,
                                "cairn.tx_scan",
                                vec![source.schema.name.as_str().into(), after.clone(), 1024.into()],
                            )
                            .await?;
                        let page = response
                            .first()
                            .and_then(Value::as_array)
                            .ok_or_else(|| DbError::new("XX000", "invalid scan response"))?;
                        for row in page {
                            let row = row
                                .as_array()
                                .ok_or_else(|| DbError::new("XX000", "invalid scan row"))?;
                            memory = memory.saturating_add(row_cost(row));
                            if memory > MAX_BYTES || rows.len() >= MAX_ROWS {
                                return Err(quota());
                            }
                            rows.push(row.clone());
                        }
                        let next = response.get(1).cloned().unwrap_or(Value::Nil);
                        if next.is_nil() {
                            break;
                        }
                        if next.as_i64().is_none()
                            || page.is_empty()
                            || (!after.is_nil() && next.as_i64() <= after.as_i64())
                        {
                            return Err(DbError::new("XX000", "scan cursor did not advance"));
                        }
                        after = next;
                    }
                }
            }
            if rows.iter().any(|row| row.len() != source.schema.columns.len()) {
                return Err(DbError::new("XX000", "tuple width differs from schema"));
            }
            check_rows(&rows)?;
            inputs.push(rows);
        }
        let plan = self.clone();
        let parameters = parameters.to_vec();
        let permit = COMPUTE.clone().acquire_owned().await.map_err(|_| quota())?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            plan.compute(inputs, &parameters)
        })
        .await
        .map_err(|error| DbError::new("XX000", format!("query execution failed: {error}")))?
    }

    fn join(&self, left: Vec<Row>, right: Vec<Row>, join: &Join, parameters: &[Value]) -> Result<Vec<Row>> {
        let source = &self.sources[join.source];
        let width = source.schema.columns.len();
        let equi = join
            .condition
            .as_ref()
            .and_then(Expression::equi_columns)
            .and_then(|(a, b)| {
                if a < source.start && b >= source.start && b < source.start + width {
                    Some((a, b - source.start))
                } else if b < source.start && a >= source.start && a < source.start + width {
                    Some((b, a - source.start))
                } else {
                    None
                }
            });
        let mut index: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        if let Some((_, column)) = equi {
            for (position, row) in right.iter().enumerate() {
                if !row[column].is_nil() {
                    index
                        .entry(expression::key(std::slice::from_ref(&row[column]))?)
                        .or_default()
                        .push(position);
                }
            }
        }
        let all = (0..right.len()).collect::<Vec<_>>();
        let mut matched_right = vec![false; right.len()];
        let mut output = Vec::new();
        let mut work = 0usize;
        let mut bytes = 0usize;
        for row in left {
            let candidates = if let Some((column, _)) = equi {
                if row[column].is_nil() {
                    &[][..]
                } else {
                    index
                        .get(&expression::key(std::slice::from_ref(&row[column]))?)
                        .map(Vec::as_slice)
                        .unwrap_or(&[])
                }
            } else {
                all.as_slice()
            };
            let mut matched = false;
            for position in candidates {
                work += 1;
                if work > MAX_JOIN_WORK {
                    return Err(quota());
                }
                let mut combined = row.clone();
                combined.extend_from_slice(&right[*position]);
                let accepted = join
                    .condition
                    .as_ref()
                    .map(|condition| {
                        condition
                            .eval(&combined, parameters, None)
                            .and_then(|v| expression::truth(&v))
                    })
                    .transpose()?
                    .flatten()
                    .unwrap_or(join.condition.is_none());
                if accepted {
                    matched = true;
                    matched_right[*position] = true;
                    bytes = bytes.saturating_add(row_cost(&combined));
                    output.push(combined);
                    if output.len() > MAX_ROWS || bytes > MAX_BYTES {
                        return Err(quota());
                    }
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                let mut combined = row;
                combined.resize(combined.len() + width, Value::Nil);
                bytes = bytes.saturating_add(row_cost(&combined));
                output.push(combined);
            }
        }
        if matches!(join.kind, JoinKind::Right | JoinKind::Full) {
            for (position, row) in right.into_iter().enumerate() {
                if !matched_right[position] {
                    let mut combined = vec![Value::Nil; source.start];
                    combined.extend(row);
                    bytes = bytes.saturating_add(row_cost(&combined));
                    output.push(combined);
                    if output.len() > MAX_ROWS || bytes > MAX_BYTES {
                        return Err(quota());
                    }
                }
            }
        }
        check_rows(&output)?;
        Ok(output)
    }

    fn compute(&self, inputs: Vec<Vec<Row>>, parameters: &[Value]) -> Result<Output> {
        let bound = |expression: &Option<Expression>, default: usize, code| -> Result<usize> {
            let Some(expression) = expression else {
                return Ok(default);
            };
            let value = expression.eval(&[], parameters, None)?;
            if value.is_nil() {
                return Ok(default);
            }
            let value = value
                .as_i64()
                .ok_or_else(|| DbError::new("42804", "LIMIT/OFFSET must be BIGINT"))?;
            if value < 0 {
                return Err(DbError::new(code, "LIMIT/OFFSET must not be negative"));
            }
            usize::try_from(value).map_err(|_| quota())
        };
        let limit = bound(&self.limit, usize::MAX, "2201W")?;
        let offset = bound(&self.offset, 0, "2201X")?;
        let mut inputs = inputs.into_iter();
        let mut rows = inputs.next().unwrap_or_else(|| vec![vec![]]);
        for join in &self.joins {
            rows = self.join(rows, inputs.next().ok_or_else(unsupported)?, join, parameters)?;
        }
        if let Some(filter) = &self.filter {
            let mut selected = Vec::new();
            for row in rows {
                if expression::truth(&filter.eval(&row, parameters, None)?)? == Some(true) {
                    selected.push(row);
                }
            }
            rows = selected;
        }
        let mut projected = Vec::new();
        let mut seen = HashSet::new();
        let mut bytes = 0usize;
        let mut emit = |row: &[Value], group: Option<&[Row]>| -> Result<()> {
            if let Some(having) = &self.having {
                if expression::truth(&having.eval(row, parameters, group)?)? != Some(true) {
                    return Ok(());
                }
            }
            let values = self
                .projection
                .iter()
                .map(|expr| expr.eval(row, parameters, group))
                .collect::<Result<Row>>()?;
            if self.distinct && !seen.insert(expression::key(&values)?) {
                return Ok(());
            }
            let keys = self
                .order
                .iter()
                .map(|order| order.expression.eval(row, parameters, group).and_then(SortValue::new))
                .collect::<Result<Vec<_>>>()?;
            bytes = bytes.saturating_add(row_cost(&values) + keys.iter().map(SortValue::cost).sum::<usize>());
            if bytes > MAX_BYTES || projected.len() >= MAX_ROWS {
                return Err(quota());
            }
            projected.push((values, keys));
            Ok(())
        };
        if self.aggregate {
            if self.groups.is_empty() {
                let empty = vec![Value::Nil; self.width];
                emit(rows.first().unwrap_or(&empty), Some(&rows))?;
            } else {
                let mut groups: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();
                for row in rows {
                    let values = self
                        .groups
                        .iter()
                        .map(|expr| expr.eval(&row, parameters, None))
                        .collect::<Result<Row>>()?;
                    groups.entry(expression::key(&values)?).or_default().push(row);
                }
                for group in groups.values() {
                    emit(&group[0], Some(group))?;
                }
            }
        } else {
            for row in rows {
                emit(&row, None)?;
            }
        }
        projected.sort_unstable_by(|a, b| {
            for (index, order) in self.order.iter().enumerate() {
                let (a, b) = (&a.1[index], &b.1[index]);
                let comparison = match (a.is_null(), b.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if order.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, true) => {
                        if order.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (false, false) => {
                        if order.ascending {
                            a.cmp(b)
                        } else {
                            a.cmp(b).reverse()
                        }
                    }
                };
                if comparison != Ordering::Equal {
                    return comparison;
                }
            }
            Ordering::Equal
        });
        let rows = projected
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(row, _)| row)
            .collect::<Vec<_>>();
        let tag = format!("SELECT {}", rows.len());
        Ok(Output { rows, tag })
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
enum SortValue {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}
impl SortValue {
    fn new(value: Value) -> Result<Self> {
        if value.is_nil() {
            Ok(Self::Null)
        } else if let Some(value) = value.as_i64() {
            Ok(Self::Integer(value))
        } else if let Some(value) = value.as_str() {
            Ok(Self::Text(value.into()))
        } else if let Some(value) = value.as_bool() {
            Ok(Self::Boolean(value))
        } else {
            Err(DbError::new("42804", "unsupported sort value"))
        }
    }
    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
    fn cost(&self) -> usize {
        std::mem::size_of::<Self>() + if let Self::Text(value) = self { value.len() } else { 0 }
    }
}
