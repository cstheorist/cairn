use crate::{
    expression::{self, Compiler, Expression, Field, Row},
    relational::{rows_cost, SelectPlan},
    sql::{self, Engine, Output, Plan},
    storage::{Column, Schema},
    transaction::Session,
    DbError, Result,
};
use rmpv::Value;
use sqlparser::ast::*;
use std::collections::HashSet;

#[derive(Clone, Debug)]
enum Change {
    Insert(Vec<Vec<Expression>>),
    Update(SelectPlan, Vec<(usize, Expression)>),
    Delete(SelectPlan),
}
#[derive(Clone, Debug)]
pub struct MutationPlan {
    schema: Schema,
    change: Change,
    returning: Vec<Expression>,
}
fn unsupported() -> DbError {
    expression::unsupported()
}
fn assignment(expression: &Expression, column: &Column) -> Result<()> {
    if expression.is_aggregate()
        || (expression.kind != column.kind && !(sql::integral(&expression.kind) && sql::integral(&column.kind)))
    {
        return Err(DbError::new(
            "42804",
            format!("incompatible assignment to {}", column.name),
        ));
    }
    Ok(())
}
fn filter(compiler: &mut Compiler, selection: &Option<Expr>) -> Result<Option<Expression>> {
    selection
        .as_ref()
        .map(|selection| {
            let expression = compiler.compile(selection, Some("boolean"))?;
            if expression.is_aggregate() {
                return Err(DbError::new("42803", "aggregate is not valid in WHERE"));
            }
            if expression.kind != "boolean" {
                return Err(DbError::new("42804", "WHERE requires BOOLEAN"));
            }
            Ok(expression)
        })
        .transpose()
}
fn relation(factor: &TableFactor) -> Result<(&ObjectName, String)> {
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
    let qualifier = alias
        .as_ref()
        .map(|alias| sql::ident(&alias.name))
        .unwrap_or_else(|| name.0.last().ok_or_else(unsupported).and_then(sql::ident))?;
    Ok((name, qualifier))
}
fn fields(schema: &Schema, qualifier: String) -> Vec<Field> {
    schema
        .columns
        .iter()
        .map(|column| Field {
            qualifier: qualifier.clone(),
            column: column.clone(),
        })
        .collect()
}
fn projection(compiler: &mut Compiler, items: &[SelectItem]) -> Result<(Vec<Expression>, Vec<Column>)> {
    let mut expressions = Vec::new();
    let mut columns = Vec::new();
    for item in items {
        let (expression, name) = match item {
            SelectItem::Wildcard(options) if options.to_string().is_empty() => {
                for (index, field) in compiler.fields.iter().enumerate() {
                    expressions.push(Expression::column_reference(index, &field.column));
                    columns.push(field.column.clone());
                }
                continue;
            }
            SelectItem::UnnamedExpr(expression) => {
                let name = match expression {
                    Expr::Identifier(id) => sql::ident(id)?,
                    Expr::CompoundIdentifier(ids) => sql::ident(ids.last().ok_or_else(unsupported)?)?,
                    Expr::Function(function) => function.name.to_string().to_ascii_lowercase(),
                    _ => "?column?".into(),
                };
                (expression, name)
            }
            SelectItem::ExprWithAlias { expr, alias } => (expr, sql::ident(alias)?),
            _ => return Err(unsupported()),
        };
        let expression = compiler.compile(expression, None)?;
        if expression.is_aggregate() {
            return Err(DbError::new("42803", "aggregates are not valid in RETURNING"));
        }
        columns.push(expression.column(name));
        expressions.push(expression);
    }
    Ok((expressions, columns))
}
fn returning_clause(items: &Option<Vec<SelectItem>>) -> String {
    items
        .as_ref()
        .map(|items| {
            format!(
                " RETURNING {}",
                items.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
            )
        })
        .unwrap_or_default()
}
pub async fn prepare(engine: &Engine, sql_text: &str) -> Result<Option<Plan>> {
    let statements = sql::parse(sql_text)?;
    let [statement] = statements.as_slice() else {
        return Ok(None);
    };
    let (schema, change, mut compiler, returning) = match statement {
        Statement::Insert(insert) => {
            let schema = engine.schema(&insert.table_name).await?;
            let source = insert.source.as_ref().ok_or_else(unsupported)?;
            let SetExpr::Values(values) = source.body.as_ref() else {
                return Err(unsupported());
            };
            if values.rows.is_empty() || values.rows.len() > 4096 {
                return Err(DbError::new("54000", "INSERT requires 1..4096 rows"));
            }
            let indices = if insert.columns.is_empty() {
                (0..schema.columns.len()).collect::<Vec<_>>()
            } else {
                insert
                    .columns
                    .iter()
                    .map(|name| sql::column_index(&schema, name))
                    .collect::<Result<Vec<_>>>()?
            };
            if indices.iter().copied().collect::<HashSet<_>>().len() != indices.len() {
                return Err(DbError::new("42701", "duplicate INSERT column"));
            }
            let mut compiler = Compiler::new(vec![]);
            let mut rows = Vec::new();
            for row in &values.rows {
                if row.len() != indices.len() {
                    return Err(DbError::new("42601", "INSERT row has the wrong number of values"));
                }
                let mut expressions = schema
                    .columns
                    .iter()
                    .map(|column| compiler.compile(&Expr::Value(sqlparser::ast::Value::Null), Some(&column.kind)))
                    .collect::<Result<Vec<_>>>()?;
                for (expression, index) in row.iter().zip(&indices) {
                    let expression = compiler.compile(expression, Some(&schema.columns[*index].kind))?;
                    assignment(&expression, &schema.columns[*index])?;
                    expressions[*index] = expression;
                }
                rows.push(expressions);
            }
            sql::exact(
                statement,
                format!(
                    "INSERT INTO {}{} VALUES {}{}",
                    insert.table_name,
                    if insert.columns.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " ({})",
                            insert
                                .columns
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    },
                    values
                        .rows
                        .iter()
                        .map(|row| format!(
                            "({})",
                            row.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
                        ))
                        .collect::<Vec<_>>()
                        .join(", "),
                    returning_clause(&insert.returning)
                ),
            )?;
            compiler.fields = fields(&schema, schema.name.clone());
            (schema, Change::Insert(rows), compiler, &insert.returning)
        }
        Statement::Update {
            table,
            assignments,
            selection,
            returning,
            ..
        } => {
            if !table.joins.is_empty() {
                return Err(unsupported());
            }
            let (name, qualifier) = relation(&table.relation)?;
            let schema = engine.schema(name).await?;
            let mut compiler = Compiler::new(fields(&schema, qualifier));
            let mut updates = Vec::new();
            let mut unique = HashSet::new();
            for update in assignments {
                let AssignmentTarget::ColumnName(name) = &update.target else {
                    return Err(unsupported());
                };
                if name.0.len() != 1 {
                    return Err(unsupported());
                }
                let index = sql::column_index(&schema, &name.0[0])?;
                if !unique.insert(index) {
                    return Err(DbError::new("42701", "duplicate UPDATE assignment"));
                }
                let expression = compiler.compile(&update.value, Some(&schema.columns[index].kind))?;
                assignment(&expression, &schema.columns[index])?;
                updates.push((index, expression));
            }
            let selection_plan = SelectPlan::table_rows(schema.clone(), filter(&mut compiler, selection)?);
            sql::exact(
                statement,
                format!(
                    "UPDATE {} SET {}{}{}",
                    table,
                    assignments
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    selection
                        .as_ref()
                        .map(|selection| format!(" WHERE {selection}"))
                        .unwrap_or_default(),
                    returning_clause(returning)
                ),
            )?;
            (schema, Change::Update(selection_plan, updates), compiler, returning)
        }
        Statement::Delete(delete) => {
            let FromTable::WithFromKeyword(from) = &delete.from else {
                return Err(unsupported());
            };
            if from.len() != 1 || !from[0].joins.is_empty() {
                return Err(unsupported());
            }
            let (name, qualifier) = relation(&from[0].relation)?;
            let schema = engine.schema(name).await?;
            let mut compiler = Compiler::new(fields(&schema, qualifier));
            let selection_plan = SelectPlan::table_rows(schema.clone(), filter(&mut compiler, &delete.selection)?);
            sql::exact(
                statement,
                format!(
                    "DELETE FROM {}{}{}",
                    from[0],
                    delete
                        .selection
                        .as_ref()
                        .map(|selection| format!(" WHERE {selection}"))
                        .unwrap_or_default(),
                    returning_clause(&delete.returning)
                ),
            )?;
            (schema, Change::Delete(selection_plan), compiler, &delete.returning)
        }
        _ => return Ok(None),
    };
    let (returning, columns) = projection(&mut compiler, returning.as_deref().unwrap_or(&[]))?;
    let parameters = compiler
        .parameters
        .into_iter()
        .map(|column| column.ok_or_else(|| DbError::new("42P02", "parameter numbers must be contiguous from $1")))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(Plan::mutation(
        MutationPlan {
            schema,
            change,
            returning,
        },
        columns,
        parameters,
        sql_text.len(),
    )))
}

impl MutationPlan {
    pub async fn execute(&self, engine: &Engine, parameters: &[Value], session: &mut Session) -> Result<Output> {
        if session
            .transaction
            .as_ref()
            .is_none_or(|transaction| transaction.read_only)
        {
            return Err(DbError::new("25006", "mutation requires a writable transaction"));
        }
        let mut rows: Vec<Row> = Vec::new();
        // Push a typed column increment into the existing durable intent path.
        // The storage operation reads the original row, checks arithmetic and
        // stages its result atomically; Sync/COMMIT still decides visibility.
        if let Change::Update(selection, assignments) = &self.change {
            if let [(index, expression)] = assignments.as_slice() {
                if self.returning.is_empty() && *index != self.schema.primary_key {
                    if let (Some(key), Some(delta)) = (selection.point_key(), expression.column_increment(*index)) {
                        let key = key.eval(&[], parameters, None)?;
                        let count = if key.is_nil() {
                            0
                        } else {
                            let key = key
                                .as_i64()
                                .ok_or_else(|| DbError::new("42804", "integer key required"))?;
                            let delta = delta.eval(&[], parameters, None)?;
                            let shard = engine.cluster.owner(key);
                            let transaction = session
                                .transaction
                                .as_mut()
                                .ok_or_else(|| DbError::new("XX000", "missing mutation transaction"))?;
                            let response = transaction
                                .call(
                                    &engine.cluster,
                                    shard,
                                    "cairn.tx_mutate",
                                    vec![
                                        self.schema.name.as_str().into(),
                                        key.into(),
                                        "add".into(),
                                        Value::Array(vec![(*index + 1).into(), delta]),
                                    ],
                                )
                                .await?;
                            usize::from(
                                response
                                    .first()
                                    .and_then(Value::as_bool)
                                    .ok_or_else(|| DbError::new("XX000", "invalid increment response"))?,
                            )
                        };
                        return Ok(Output {
                            rows,
                            tag: format!("UPDATE {count}"),
                        });
                    }
                }
            }
        }
        let mut changes = Vec::new();
        match &self.change {
            Change::Insert(expressions) => {
                for row in expressions {
                    let row = row
                        .iter()
                        .zip(&self.schema.columns)
                        .map(|(expression, column)| {
                            sql::validate_value(expression.eval(&[], parameters, None)?, column)
                        })
                        .collect::<Result<Row>>()?;
                    changes.push((None, Some(row)));
                }
            }
            Change::Update(selection, assignments) => {
                let original = selection.execute(engine, parameters, session).await?.rows;
                if original.len() > 1 && assignments.iter().any(|(index, _)| *index == self.schema.primary_key) {
                    return Err(DbError::unsupported(
                        "primary key moves currently require a single matching row",
                    ));
                }
                for old in original {
                    let mut row = old.clone();
                    for (index, expression) in assignments {
                        row[*index] = sql::validate_value(
                            expression.eval(&old, parameters, None)?,
                            &self.schema.columns[*index],
                        )?;
                    }
                    changes.push((Some(old), Some(row)));
                }
            }
            Change::Delete(selection) => {
                for old in selection.execute(engine, parameters, session).await?.rows {
                    changes.push((Some(old), None));
                }
            }
        }
        if changes.len() > 4096
            || changes
                .iter()
                .map(|(old, new)| {
                    old.as_ref().map_or(0, |row| rows_cost(std::slice::from_ref(row)))
                        + new.as_ref().map_or(0, |row| rows_cost(std::slice::from_ref(row)))
                })
                .sum::<usize>()
                > 64 * 1024 * 1024
        {
            return Err(DbError::new("54000", "mutation exceeds the row or memory budget"));
        }
        let count = changes.len();
        for (old, new) in changes {
            let image = new
                .as_ref()
                .or(old.as_ref())
                .ok_or_else(|| DbError::new("XX000", "missing mutation image"))?;
            if !self.returning.is_empty() {
                rows.push(
                    self.returning
                        .iter()
                        .map(|expression| expression.eval(image, parameters, None))
                        .collect::<Result<Row>>()?,
                );
            }
            let old_key = old.as_ref().and_then(|row| row[self.schema.primary_key].as_i64());
            let new_key = new.as_ref().and_then(|row| row[self.schema.primary_key].as_i64());
            if let Some(key) = old_key.filter(|_| old_key != new_key) {
                engine
                    .data_call(
                        session,
                        key,
                        "cairn.delete",
                        vec![self.schema.name.as_str().into(), key.into()],
                    )
                    .await?;
            }
            if let Some(row) = new {
                let key = new_key.ok_or_else(|| DbError::new("23502", "NULL primary key"))?;
                if old_key != new_key {
                    engine
                        .data_call(
                            session,
                            key,
                            "cairn.insert",
                            vec![self.schema.name.as_str().into(), Value::Array(row)],
                        )
                        .await?;
                } else {
                    let updates = row
                        .into_iter()
                        .enumerate()
                        .filter(|(index, _)| *index != self.schema.primary_key)
                        .map(|(index, value)| Value::Array(vec!["=".into(), (index + 1).into(), value]))
                        .collect();
                    engine
                        .data_call(
                            session,
                            key,
                            "cairn.update",
                            vec![self.schema.name.as_str().into(), key.into(), Value::Array(updates)],
                        )
                        .await?;
                }
            }
            if rows_cost(&rows) > 16 * 1024 * 1024 {
                return Err(DbError::new("54000", "RETURNING exceeds the memory budget"));
            }
        }
        let tag = match self.change {
            Change::Insert(_) => format!("INSERT 0 {count}"),
            Change::Update(..) => format!("UPDATE {count}"),
            Change::Delete(_) => format!("DELETE {count}"),
        };
        Ok(Output { rows, tag })
    }
}
