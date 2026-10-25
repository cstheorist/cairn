use crate::{
    storage::{Cluster, Column, Schema},
    transaction::Session,
    DbError, Result,
};
use rmpv::Value;
use sqlparser::{
    ast::*,
    dialect::PostgreSqlDialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, Tokenizer, Word},
};
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
enum Input {
    Literal(Value),
    Parameter(usize, Column),
}

#[derive(Clone, Debug)]
enum Action {
    Empty,
    Begin(bool),
    Commit,
    Rollback,
    Deallocate(String),
    Select(Box<crate::relational::SelectPlan>),
    Mutation(Box<crate::mutation::MutationPlan>),
    Create(Schema, bool),
    Insert(Schema, Vec<Input>),
    Get(Schema, Input, Vec<usize>),
    Delete(Schema, Input),
    Constant(Vec<Value>),
}

#[derive(Clone, Debug)]
pub struct Plan {
    action: Action,
    pub columns: Vec<Column>,
    pub parameters: Vec<Column>,
    pub memory_cost: usize,
}

impl Plan {
    pub(crate) fn mutation(
        mutation: crate::mutation::MutationPlan,
        columns: Vec<Column>,
        parameters: Vec<Column>,
        bytes: usize,
    ) -> Self {
        Self {
            action: Action::Mutation(Box::new(mutation)),
            columns,
            parameters,
            memory_cost: bytes.saturating_mul(4).saturating_add(65536),
        }
    }
    pub(crate) fn select(
        select: crate::relational::SelectPlan,
        columns: Vec<Column>,
        parameters: Vec<Column>,
        bytes: usize,
    ) -> Self {
        Self {
            action: Action::Select(Box::new(select)),
            columns,
            parameters,
            memory_cost: bytes.saturating_mul(4).saturating_add(65536),
        }
    }
    pub fn needs_simple_transaction(&self) -> bool {
        matches!(&self.action, Action::Select(select) if select.needs_transaction())
            || matches!(&self.action, Action::Mutation(_))
    }
    pub fn deallocation_target(&self) -> Option<&str> {
        if let Action::Deallocate(name) = &self.action {
            Some(name)
        } else {
            None
        }
    }
    pub fn is_row_mutation(&self) -> bool {
        self.needs_simple_transaction() || matches!(self.action, Action::Insert(..) | Action::Delete(..))
    }
}

pub struct Output {
    pub rows: Vec<Vec<Value>>,
    pub tag: String,
}

pub struct Engine {
    pub cluster: std::sync::Arc<Cluster>,
    catalog: RwLock<HashMap<String, Schema>>,
}

fn unsupported() -> DbError {
    DbError::unsupported("this SQL statement or clause is not supported")
}

pub(crate) fn ident(id: &Ident) -> Result<String> {
    if id.quote_style.is_some() {
        return Err(DbError::unsupported("quoted identifiers are not supported"));
    }
    let value = id.value.to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 63
        || !value.as_bytes()[0].is_ascii_lowercase()
        || !value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
    {
        return Err(DbError::new(
            "42602",
            "identifiers must match [a-z][a-z0-9_]* and fit in 63 bytes",
        ));
    }
    Ok(value)
}

fn table_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(DbError::unsupported("schemas and qualified names are not supported"));
    }
    let name = ident(&name.0[0])?;
    if name.starts_with("cairn") {
        return Err(DbError::new("42602", "cairn prefix is reserved"));
    }
    Ok(name)
}

// Re-render an allowlisted statement and compare the complete normalized SQL.
// This rejects every clause/option that this planner did not deliberately handle.
pub(crate) fn exact(statement: &Statement, allowed: String) -> Result<()> {
    let ast = parse(&allowed).map_err(|_| unsupported())?;
    if ast.len() != 1 || ast[0].to_string() != statement.to_string() {
        return Err(unsupported());
    }
    Ok(())
}

pub(crate) fn parse(sql: &str) -> Result<Vec<Statement>> {
    let dialect = PostgreSqlDialect {};
    let mut tokens = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|error| DbError::new("42601", error.to_string()))?;
    // sqlparser 0.53 omits RETURNING from its table-alias reserved words.
    // Preserve every original token and insert the equivalent WHERE TRUE only
    // for an unqualified DELETE ... RETURNING at the outer statement level.
    let mut delete = false;
    let mut first = true;
    let mut depth = 0;
    let mut has_where = false;
    let mut position = None;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Token::Whitespace(_) => continue,
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::SemiColon => {
                first = true;
                delete = false;
                has_where = false;
                continue;
            }
            Token::Word(word) if depth == 0 => {
                if first {
                    delete = word.keyword == Keyword::DELETE;
                }
                if word.keyword == Keyword::WHERE {
                    has_where = true;
                }
                if delete && word.keyword == Keyword::RETURNING && !has_where {
                    position = Some(index);
                    break;
                }
            }
            _ => {}
        }
        first = false;
    }
    if let Some(position) = position {
        let word = |value: &str, keyword| {
            Token::Word(Word {
                value: value.into(),
                quote_style: None,
                keyword,
            })
        };
        tokens.splice(
            position..position,
            [word("WHERE", Keyword::WHERE), word("TRUE", Keyword::TRUE)],
        );
    }
    Parser::new(&dialect)
        .with_tokens(tokens)
        .parse_statements()
        .map_err(|error| DbError::new("42601", error.to_string()))
}

pub(crate) fn column_index(schema: &Schema, column: &Ident) -> Result<usize> {
    let name = ident(column)?;
    schema
        .columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| DbError::new("42703", format!("column {name} does not exist")))
}

pub(crate) fn kind(data_type: &DataType) -> Result<&'static str> {
    match data_type {
        DataType::BigInt(None) => Ok("bigint"),
        DataType::Int(None) | DataType::Integer(None) => Ok("integer"),
        DataType::SmallInt(None) => Ok("smallint"),
        DataType::Text => Ok("text"),
        DataType::Boolean | DataType::Bool => Ok("boolean"),
        _ => Err(DbError::unsupported(
            "supported types: SMALLINT, INTEGER, BIGINT, TEXT, BOOLEAN",
        )),
    }
}

pub fn text_value(text: Option<&str>, column: &Column) -> Result<Value> {
    let Some(text) = text else {
        return if column.nullable {
            Ok(Value::Nil)
        } else {
            Err(DbError::new("23502", format!("{} cannot be NULL", column.name)))
        };
    };
    match column.kind.as_str() {
        "bigint" | "integer" | "smallint" => {
            let value = text.trim().parse::<i64>().map_err(|error| {
                let state = match error.kind() {
                    std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => "22003",
                    _ => "22P02",
                };
                DbError::new(state, format!("invalid {} value", column.kind))
            })?;
            integer_value(value, &column.kind)
        }
        "boolean" => match text {
            "true" | "t" | "1" => Ok(true.into()),
            "false" | "f" | "0" => Ok(false.into()),
            _ => Err(DbError::new("22P02", "invalid BOOLEAN value")),
        },
        _ => Ok(text.into()),
    }
}

pub fn integral(kind: &str) -> bool {
    matches!(kind, "smallint" | "integer" | "bigint")
}

pub(crate) fn integer_value(value: i64, kind: &str) -> Result<Value> {
    if (kind == "smallint" && i16::try_from(value).is_err()) || (kind == "integer" && i32::try_from(value).is_err()) {
        return Err(DbError::new("22003", format!("{kind} value out of range")));
    }
    Ok(value.into())
}

pub(crate) fn validate_value(value: Value, column: &Column) -> Result<Value> {
    if value.is_nil() {
        return text_value(None, column);
    }
    let valid = match column.kind.as_str() {
        "bigint" | "integer" | "smallint" => {
            return integer_value(
                value
                    .as_i64()
                    .ok_or_else(|| DbError::new("42804", "expected an integer"))?,
                &column.kind,
            );
        }
        "boolean" => value.as_bool().is_some(),
        _ => value.as_str().is_some(),
    };
    if valid {
        Ok(value)
    } else {
        Err(DbError::new("42804", format!("invalid value type for {}", column.name)))
    }
}

fn input(expression: &Expr, column: &Column, parameters: &mut Vec<Option<Column>>) -> Result<Input> {
    if let Expr::Value(sqlparser::ast::Value::Placeholder(placeholder)) = expression {
        let index = placeholder
            .strip_prefix('$')
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|i| *i > 0 && *i <= 128)
            .ok_or_else(|| DbError::new("42P02", "expected parameter $1..$128"))?
            - 1;
        parameters.resize(parameters.len().max(index + 1), None);
        if let Some(previous) = &parameters[index] {
            if previous.kind != column.kind {
                return Err(DbError::new("42P08", "parameter has conflicting types"));
            }
        } else {
            parameters[index] = Some(column.clone());
        }
        return Ok(Input::Parameter(index, column.clone()));
    }
    let value = match expression {
        Expr::Value(sqlparser::ast::Value::Null) => text_value(None, column)?,
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(value)) => text_value(Some(value), column)?,
        Expr::Value(sqlparser::ast::Value::Boolean(value)) => validate_value((*value).into(), column)?,
        Expr::Value(sqlparser::ast::Value::Number(value, false)) if integral(&column.kind) => {
            text_value(Some(value), column)?
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } if integral(&column.kind) => {
            if let Expr::Value(sqlparser::ast::Value::Number(number, false)) = expr.as_ref() {
                text_value(Some(&format!("-{number}")), column)?
            } else {
                return Err(unsupported());
            }
        }
        _ => return Err(unsupported()),
    };
    Ok(Input::Literal(value))
}

fn key_input(selection: &Option<Expr>, schema: &Schema, parameters: &mut Vec<Option<Column>>) -> Result<Input> {
    let Some(Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    }) = selection
    else {
        return Err(unsupported());
    };
    let Expr::Identifier(key) = left.as_ref() else {
        return Err(unsupported());
    };
    if column_index(schema, key)? != schema.primary_key {
        return Err(DbError::unsupported("WHERE must be equality on the primary key"));
    }
    let mut predicate_column = schema.columns[schema.primary_key].clone();
    predicate_column.nullable = true;
    input(right, &predicate_column, parameters)
}

fn plain_table(factor: &TableFactor) -> Result<&ObjectName> {
    match factor {
        TableFactor::Table {
            name,
            alias: None,
            args: None,
            with_hints,
            version: None,
            partitions,
            ..
        } if with_hints.is_empty() && partitions.is_empty() => Ok(name),
        _ => Err(unsupported()),
    }
}

impl Input {
    fn resolve(&self, parameters: &[Value]) -> Result<Value> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Parameter(index, column) => validate_value(
                parameters
                    .get(*index)
                    .cloned()
                    .ok_or_else(|| DbError::new("08P01", "missing bind parameter"))?,
                column,
            ),
        }
    }
}

impl Engine {
    pub fn new(cluster: cairn_iproto::Cluster) -> Self {
        Self {
            cluster: std::sync::Arc::new(Cluster(std::sync::Arc::new(cluster))),
            catalog: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) async fn schema(&self, name: &ObjectName) -> Result<Schema> {
        let name = table_name(name)?;
        if let Some(schema) = self.catalog.read().await.get(&name) {
            return Ok(schema.clone());
        }
        let schema = self.cluster.schema(&name).await?;
        self.catalog.write().await.insert(name, schema.clone());
        Ok(schema)
    }

    pub async fn prepare(&self, sql: &str) -> Result<Plan> {
        match self.prepare_fast(sql).await {
            Err(error) if error.code == "0A000" => match crate::mutation::prepare(self, sql).await? {
                Some(plan) => Ok(plan),
                None => crate::relational::prepare(self, sql).await,
            },
            result => result,
        }
    }

    async fn prepare_fast(&self, sql: &str) -> Result<Plan> {
        let statements = parse(sql)?;
        if statements.is_empty() {
            return Ok(Plan {
                action: Action::Empty,
                columns: vec![],
                parameters: vec![],
                memory_cost: 4096,
            });
        }
        if statements.len() != 1 {
            return Err(DbError::unsupported(
                "one SQL statement per request is required; implicit multi-statement transactions are not implemented",
            ));
        }
        let statement = &statements[0];
        let mut parameters = Vec::new();
        let mut columns = Vec::new();
        let action = match statement {
            Statement::StartTransaction { .. } => {
                let allowed = [
                    "BEGIN",
                    "START TRANSACTION",
                    "BEGIN ISOLATION LEVEL SERIALIZABLE",
                    "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                    "BEGIN ISOLATION LEVEL READ COMMITTED",
                    "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
                    "BEGIN ISOLATION LEVEL REPEATABLE READ",
                    "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                    "BEGIN READ ONLY",
                    "START TRANSACTION READ ONLY",
                    "BEGIN READ WRITE",
                    "START TRANSACTION READ WRITE",
                ];
                if !allowed
                    .iter()
                    .any(|allowed| exact(statement, (*allowed).into()).is_ok())
                {
                    return Err(unsupported());
                }
                Action::Begin(statement.to_string().ends_with("READ ONLY"))
            }
            Statement::Commit { .. } => {
                exact(statement, "COMMIT".into())?;
                Action::Commit
            }
            Statement::Rollback { .. } => {
                exact(statement, "ROLLBACK".into())?;
                Action::Rollback
            }
            Statement::Deallocate { name, .. } => Action::Deallocate(if name.quote_style.is_none() {
                name.value.to_ascii_lowercase()
            } else {
                name.value.clone()
            }),
            Statement::CreateTable(create) => {
                let name = table_name(&create.name)?;
                if create.columns.is_empty() || create.columns.len() > 128 {
                    return Err(DbError::new("54000", "expected 1..128 columns"));
                }
                let mut names = HashSet::new();
                let mut definitions = Vec::new();
                let mut allowed_columns = Vec::new();
                let mut primary_key = None;
                for (index, definition) in create.columns.iter().enumerate() {
                    let name = ident(&definition.name)?;
                    if !names.insert(name.clone()) {
                        return Err(DbError::new("42701", "duplicate column name"));
                    }
                    let mut nullable = true;
                    let mut null_option = false;
                    for option in &definition.options {
                        if option.name.is_some() {
                            return Err(unsupported());
                        }
                        match &option.option {
                            ColumnOption::Unique { is_primary: true, .. } => {
                                if option.option.to_string() != "PRIMARY KEY" {
                                    return Err(unsupported());
                                }
                                if primary_key.replace(index).is_some() {
                                    return Err(DbError::new("42P16", "exactly one primary key is required"));
                                }
                            }
                            ColumnOption::NotNull if !null_option => {
                                null_option = true;
                                nullable = false;
                            }
                            ColumnOption::Null if !null_option => {
                                null_option = true;
                            }
                            _ => return Err(unsupported()),
                        }
                    }
                    if primary_key == Some(index) {
                        nullable = false;
                    }
                    if definition.collation.is_some() {
                        return Err(unsupported());
                    }
                    allowed_columns.push(format!(
                        "{} {}{}",
                        definition.name,
                        definition.data_type,
                        definition
                            .options
                            .iter()
                            .map(|option| format!(" {}", option.option))
                            .collect::<String>()
                    ));
                    definitions.push(Column {
                        name,
                        kind: kind(&definition.data_type)?.into(),
                        nullable,
                    });
                }
                let pk =
                    primary_key.ok_or_else(|| DbError::unsupported("an inline integer PRIMARY KEY is required"))?;
                if !integral(&definitions[pk].kind) {
                    return Err(DbError::unsupported("primary key must be an integer type"));
                }
                exact(
                    statement,
                    format!(
                        "CREATE TABLE {}{} ({})",
                        if create.if_not_exists { "IF NOT EXISTS " } else { "" },
                        create.name,
                        allowed_columns.join(", ")
                    ),
                )?;
                Action::Create(
                    Schema {
                        name,
                        columns: definitions,
                        primary_key: pk,
                    },
                    create.if_not_exists,
                )
            }
            Statement::Insert(insert) => {
                let schema = self.schema(&insert.table_name).await?;
                let source = insert.source.as_ref().ok_or_else(unsupported)?;
                let SetExpr::Values(values) = source.body.as_ref() else {
                    return Err(unsupported());
                };
                if values.rows.len() != 1 {
                    return Err(DbError::unsupported("only one row per INSERT is supported"));
                }
                let order = if insert.columns.is_empty() {
                    (0..schema.columns.len()).collect::<Vec<_>>()
                } else {
                    insert
                        .columns
                        .iter()
                        .map(|c| column_index(&schema, c))
                        .collect::<Result<Vec<_>>>()?
                };
                if order.len() != values.rows[0].len() {
                    return Err(DbError::new("42601", "INSERT column/value count differs"));
                }
                let mut unique = HashSet::new();
                let mut row = vec![Input::Literal(Value::Nil); schema.columns.len()];
                for (index, expression) in order.iter().zip(&values.rows[0]) {
                    if !unique.insert(*index) {
                        return Err(DbError::new("42701", "duplicate INSERT column"));
                    }
                    row[*index] = input(expression, &schema.columns[*index], &mut parameters)?;
                }
                for (index, column) in schema.columns.iter().enumerate() {
                    if !unique.contains(&index) {
                        text_value(None, column)?;
                    }
                }
                exact(
                    statement,
                    format!(
                        "INSERT INTO {}{} VALUES ({})",
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
                        values.rows[0]
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                )?;
                Action::Insert(schema, row)
            }
            Statement::Query(query) => {
                let SetExpr::Select(select) = query.body.as_ref() else {
                    return Err(unsupported());
                };
                if select.from.is_empty() {
                    // Useful health probe, with the same explicit subset rules as table queries.
                    exact(statement, "SELECT 1".into())?;
                    columns.push(Column {
                        name: "?column?".into(),
                        kind: "integer".into(),
                        nullable: false,
                    });
                    Action::Constant(vec![1.into()])
                } else {
                    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
                        return Err(unsupported());
                    }
                    let name = plain_table(&select.from[0].relation)?;
                    let schema = self.schema(name).await?;
                    let key = key_input(&select.selection, &schema, &mut parameters)?;
                    let mut indices = Vec::new();
                    for projection in &select.projection {
                        match projection {
                            SelectItem::Wildcard(options) if options.to_string().is_empty() => {
                                for (index, column) in schema.columns.iter().enumerate() {
                                    indices.push(index);
                                    columns.push(column.clone());
                                }
                            }
                            SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                                let index = column_index(&schema, id)?;
                                indices.push(index);
                                columns.push(schema.columns[index].clone());
                            }
                            SelectItem::ExprWithAlias {
                                expr: Expr::Identifier(id),
                                alias,
                            } => {
                                let index = column_index(&schema, id)?;
                                indices.push(index);
                                let mut column = schema.columns[index].clone();
                                column.name = ident(alias)?;
                                columns.push(column);
                            }
                            _ => return Err(unsupported()),
                        }
                    }
                    exact(
                        statement,
                        format!(
                            "SELECT {} FROM {} WHERE {}",
                            select
                                .projection
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", "),
                            name,
                            select.selection.as_ref().unwrap()
                        ),
                    )?;
                    Action::Get(schema, key, indices)
                }
            }
            Statement::Delete(delete) => {
                let from = match &delete.from {
                    FromTable::WithFromKeyword(tables) => tables,
                    _ => return Err(unsupported()),
                };
                if from.len() != 1 || !from[0].joins.is_empty() {
                    return Err(unsupported());
                }
                let name = plain_table(&from[0].relation)?;
                let schema = self.schema(name).await?;
                let key = key_input(&delete.selection, &schema, &mut parameters)?;
                exact(
                    statement,
                    format!("DELETE FROM {} WHERE {}", name, delete.selection.as_ref().unwrap()),
                )?;
                Action::Delete(schema, key)
            }
            _ => return Err(unsupported()),
        };
        let parameters = parameters
            .into_iter()
            .map(|c| c.ok_or_else(|| DbError::new("42P02", "parameter numbers must be contiguous from $1")))
            .collect::<Result<Vec<_>>>()?;
        Ok(Plan {
            action,
            columns,
            parameters,
            memory_cost: sql.len().saturating_mul(4).saturating_add(65536),
        })
    }

    pub(crate) async fn data_call(
        &self,
        session: &mut Session,
        key: i64,
        function: &str,
        args: Vec<Value>,
    ) -> Result<Vec<Value>> {
        let shard = self.cluster.owner(key);
        if let Some(transaction) = &mut session.transaction {
            if function == "cairn.get" {
                return transaction.call(&self.cluster, shard, "cairn.tx_read", args).await;
            }
            if transaction.read_only {
                return Err(DbError::new(
                    "25006",
                    "cannot execute a mutation in a read-only transaction",
                ));
            }
            let name = args[0].clone();
            let (operation, value) = match function {
                "cairn.insert" => ("insert", args[1].clone()),
                "cairn.delete" => ("delete", Value::Nil),
                "cairn.update" => ("update", args[2].clone()),
                _ => return Err(DbError::new("XX000", "unsupported transactional operation")),
            };
            return transaction
                .call(
                    &self.cluster,
                    shard,
                    "cairn.tx_mutate",
                    vec![name, key.into(), operation.into(), value],
                )
                .await;
        }
        if function == "cairn.get" {
            self.cluster.read(shard, args).await
        } else {
            self.cluster.call(shard, function, args).await
        }
    }

    pub async fn execute(&self, plan: &Plan, parameters: &[Value], session: &mut Session) -> Result<Output> {
        if parameters.len() != plan.parameters.len() {
            return Err(DbError::new(
                "08P01",
                "bind parameter count differs from prepared statement",
            ));
        }
        let control = match plan.action {
            Action::Begin(read_only) => {
                session.begin(read_only)?;
                Some("BEGIN")
            }
            Action::Commit => Some(session.finish(&self.cluster, true).await?),
            Action::Rollback => Some(session.finish(&self.cluster, false).await?),
            _ => None,
        };
        if let Some(tag) = control {
            return Ok(Output {
                rows: vec![],
                tag: tag.into(),
            });
        }
        if session.failed {
            return Err(DbError::new("25P02", "current transaction is aborted; issue ROLLBACK"));
        }
        let mut rows = vec![];
        let tag = match &plan.action {
            Action::Empty => String::new(),
            Action::Select(select) => return select.execute(self, parameters, session).await,
            Action::Mutation(mutation) => return mutation.execute(self, parameters, session).await,
            Action::Deallocate(_) => "DEALLOCATE".into(),
            Action::Create(schema, if_not_exists) => {
                if session.transaction.is_some() {
                    return Err(DbError::unsupported("transactional DDL is not implemented yet"));
                }
                self.cluster.create(schema, *if_not_exists).await?;
                self.catalog.write().await.insert(schema.name.clone(), schema.clone());
                "CREATE TABLE".into()
            }
            Action::Insert(schema, values) => {
                let row = values
                    .iter()
                    .map(|v| v.resolve(parameters))
                    .collect::<Result<Vec<_>>>()?;
                let key = row[schema.primary_key]
                    .as_i64()
                    .ok_or_else(|| DbError::new("23502", "primary key must be a nonnull BIGINT"))?;
                self.data_call(
                    session,
                    key,
                    "cairn.insert",
                    vec![schema.name.as_str().into(), Value::Array(row)],
                )
                .await?;
                "INSERT 0 1".into()
            }
            Action::Get(schema, key, indices) => {
                let value = key.resolve(parameters)?;
                if value.is_nil() {
                    return Ok(Output {
                        rows,
                        tag: "SELECT 0".into(),
                    });
                }
                let key = value
                    .as_i64()
                    .ok_or_else(|| DbError::new("42804", "expected an integer key"))?;
                let result = self
                    .data_call(session, key, "cairn.get", vec![schema.name.as_str().into(), key.into()])
                    .await?;
                let row = result
                    .first()
                    .and_then(Value::as_array)
                    .ok_or_else(|| DbError::new("XX000", "invalid tuple response"))?;
                if !row.is_empty() {
                    if row.len() != schema.columns.len() {
                        return Err(DbError::new("XX000", "tuple width differs from catalog"));
                    }
                    rows.push(indices.iter().map(|index| row[*index].clone()).collect());
                }
                format!("SELECT {}", rows.len())
            }
            Action::Delete(schema, key) => {
                let value = key.resolve(parameters)?;
                if value.is_nil() {
                    return Ok(Output {
                        rows,
                        tag: "DELETE 0".into(),
                    });
                }
                let key = value
                    .as_i64()
                    .ok_or_else(|| DbError::new("42804", "expected an integer key"))?;
                let data = self
                    .data_call(
                        session,
                        key,
                        "cairn.delete",
                        vec![schema.name.as_str().into(), key.into()],
                    )
                    .await?;
                format!(
                    "DELETE {}",
                    usize::from(data.first().and_then(Value::as_bool).unwrap_or(false))
                )
            }
            Action::Constant(values) => {
                rows.push(values.clone());
                "SELECT 1".into()
            }
            Action::Begin(_) | Action::Commit | Action::Rollback => {
                return Err(DbError::new("XX000", "unhandled transaction control"))
            }
        };
        Ok(Output { rows, tag })
    }
}
