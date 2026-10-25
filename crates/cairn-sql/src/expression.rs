use crate::{
    core,
    sql::{integer_value, integral, text_value},
    storage::Column,
    DbError, Result,
};
use rmpv::Value;
use sqlparser::ast::{
    self, BinaryOperator as Binary, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator as Unary,
};
use std::cmp::Ordering;

pub type Row = Vec<Value>;

#[derive(Clone, Debug)]
pub struct Field {
    pub qualifier: String,
    pub column: Column,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Expression {
    pub kind: String,
    node: Node,
}

#[derive(Clone, Debug, PartialEq)]
enum Node {
    Column(usize),
    Literal(Value),
    Parameter(usize),
    Binary(Binary, Box<Expression>, Box<Expression>),
    Unary(Unary, Box<Expression>),
    IsNull(Box<Expression>, bool),
    In(Box<Expression>, Vec<Expression>, bool),
    Function(String, Vec<Expression>),
    Aggregate(String, Option<Box<Expression>>, bool),
    Cast(Box<Expression>),
}

pub struct Compiler {
    pub fields: Vec<Field>,
    pub parameters: Vec<Option<Column>>,
}

pub fn unsupported() -> DbError {
    DbError::unsupported("this SQL expression or clause is not implemented")
}
fn mismatch() -> DbError {
    DbError::new("42804", "incompatible SQL expression types")
}
fn overflow() -> DbError {
    DbError::new("22003", "BIGINT arithmetic overflow")
}
fn boxed(expression: Expression) -> Box<Expression> {
    Box::new(expression)
}
fn common_type(left: &str, right: &str) -> Result<String> {
    if left == right {
        Ok(left.into())
    } else if integral(left) && integral(right) {
        Ok(if left == "bigint" || right == "bigint" {
            "bigint"
        } else {
            "integer"
        }
        .into())
    } else {
        Err(mismatch())
    }
}
fn unknown(expression: &Expr) -> bool {
    matches!(
        expression,
        Expr::Value(ast::Value::Null | ast::Value::Placeholder(_) | ast::Value::SingleQuotedString(_))
    )
}
fn natural_integer(value: i64) -> &'static str {
    if i32::try_from(value).is_ok() {
        "integer"
    } else {
        "bigint"
    }
}

impl Compiler {
    pub fn new(fields: Vec<Field>) -> Self {
        Self {
            fields,
            parameters: vec![],
        }
    }

    fn field(&self, path: &[ast::Ident]) -> Result<usize> {
        if path.is_empty() || path.len() > 2 {
            return Err(unsupported());
        }
        let normalize = |id: &ast::Ident| {
            if id.quote_style.is_some() {
                id.value.clone()
            } else {
                id.value.to_ascii_lowercase()
            }
        };
        let name = normalize(path.last().unwrap());
        let qualifier = if path.len() == 2 {
            Some(normalize(&path[0]))
        } else {
            None
        };
        let matches = self
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.column.name == name && qualifier.as_ref().is_none_or(|q| field.qualifier == *q))
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [index] => Ok(*index),
            [] => Err(DbError::new("42703", format!("column {name} does not exist"))),
            _ => Err(DbError::new("42702", format!("column {name} is ambiguous"))),
        }
    }

    pub fn compile(&mut self, expression: &Expr, expected: Option<&str>) -> Result<Expression> {
        let make = |kind: &str, node| Expression {
            kind: kind.into(),
            node,
        };
        Ok(match expression {
            Expr::Identifier(id) => {
                let index = self.field(std::slice::from_ref(id))?;
                make(&self.fields[index].column.kind, Node::Column(index))
            }
            Expr::CompoundIdentifier(path) => {
                let index = self.field(path)?;
                make(&self.fields[index].column.kind, Node::Column(index))
            }
            Expr::Nested(inner) => return self.compile(inner, expected),
            Expr::Value(ast::Value::Placeholder(name)) => {
                let index = name
                    .strip_prefix('$')
                    .and_then(|n| n.parse::<usize>().ok())
                    .filter(|n| (1..=128).contains(n))
                    .ok_or_else(|| DbError::new("42P02", "expected parameter $1..$128"))?
                    - 1;
                self.parameters.resize(self.parameters.len().max(index + 1), None);
                let kind = expected.unwrap_or("text").to_owned();
                if let Some(previous) = &self.parameters[index] {
                    if previous.kind != kind {
                        return Err(DbError::new("42P08", "parameter has conflicting types"));
                    }
                } else {
                    self.parameters[index] = Some(Column {
                        name: name.clone(),
                        kind: kind.clone(),
                        nullable: true,
                    });
                }
                make(&kind, Node::Parameter(index))
            }
            Expr::Value(value) => {
                let natural = match value {
                    ast::Value::Number(number, false) => {
                        natural_integer(number.parse::<i64>().map_err(|_| overflow())?)
                    }
                    ast::Value::Boolean(_) => "boolean",
                    _ => "text",
                };
                let kind = if matches!(value, ast::Value::Number(..)) {
                    natural
                } else {
                    expected.unwrap_or(natural)
                };
                let value = match value {
                    ast::Value::Null => Value::Nil,
                    ast::Value::SingleQuotedString(text) => text_value(
                        Some(text),
                        &Column {
                            name: String::new(),
                            kind: kind.into(),
                            nullable: true,
                        },
                    )?,
                    ast::Value::Boolean(value) if kind == "boolean" => (*value).into(),
                    ast::Value::Number(number, false) if integral(kind) => {
                        number.parse::<i64>().map(Value::from).map_err(|_| overflow())?
                    }
                    _ => return Err(mismatch()),
                };
                make(kind, Node::Literal(value))
            }
            Expr::UnaryOp { op, expr } => {
                if *op == Unary::Minus {
                    if let Expr::Value(ast::Value::Number(number, false)) = expr.as_ref() {
                        let value = format!("-{number}").parse::<i64>().map_err(|_| overflow())?;
                        return Ok(make(natural_integer(value), Node::Literal(value.into())));
                    }
                }
                let hint = match op {
                    Unary::Not => Some("boolean"),
                    Unary::Minus | Unary::Plus => None,
                    _ => return Err(unsupported()),
                };
                let inner = self.compile(expr, hint)?;
                if hint.is_some_and(|hint| inner.kind != hint) || (hint.is_none() && !integral(&inner.kind)) {
                    return Err(mismatch());
                }
                make(&inner.kind.clone(), Node::Unary(*op, boxed(inner)))
            }
            Expr::BinaryOp { left, op, right } => {
                let boolean = matches!(op, Binary::And | Binary::Or);
                let arithmetic = matches!(
                    op,
                    Binary::Plus | Binary::Minus | Binary::Multiply | Binary::Divide | Binary::Modulo
                );
                let comparison = matches!(
                    op,
                    Binary::Eq | Binary::NotEq | Binary::Lt | Binary::LtEq | Binary::Gt | Binary::GtEq
                );
                if !boolean && !arithmetic && !comparison && *op != Binary::StringConcat {
                    return Err(unsupported());
                }
                let hint = if boolean {
                    Some("boolean")
                } else if *op == Binary::StringConcat {
                    Some("text")
                } else {
                    None
                };
                // Infer an untyped parameter from the opposite operand of a comparison.
                let (left, right) = if unknown(left) && hint.is_none() {
                    let right = self.compile(right, None)?;
                    (self.compile(left, Some(&right.kind))?, right)
                } else {
                    let left = self.compile(left, hint)?;
                    let right = self.compile(right, Some(&left.kind))?;
                    (left, right)
                };
                let common = common_type(&left.kind, &right.kind)?;
                if hint.is_some_and(|hint| common != hint) || (arithmetic && !integral(&common)) {
                    return Err(mismatch());
                }
                let kind = if boolean || comparison {
                    "boolean".into()
                } else if arithmetic {
                    common
                } else {
                    "text".into()
                };
                make(&kind, Node::Binary(op.clone(), boxed(left), boxed(right)))
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => make(
                "boolean",
                Node::IsNull(
                    boxed(self.compile(inner, None)?),
                    matches!(expression, Expr::IsNotNull(_)),
                ),
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let value = self.compile(expr, None)?;
                let low = self.compile(low, Some(&value.kind))?;
                let high = self.compile(high, Some(&value.kind))?;
                common_type(&value.kind, &low.kind)?;
                common_type(&value.kind, &high.kind)?;
                let test = make(
                    "boolean",
                    Node::Binary(
                        Binary::And,
                        boxed(make(
                            "boolean",
                            Node::Binary(Binary::GtEq, boxed(value.clone()), boxed(low)),
                        )),
                        boxed(make("boolean", Node::Binary(Binary::LtEq, boxed(value), boxed(high)))),
                    ),
                );
                if *negated {
                    make("boolean", Node::Unary(Unary::Not, boxed(test)))
                } else {
                    test
                }
            }
            Expr::InList { expr, list, negated } => {
                let value = self.compile(expr, None)?;
                let options = list
                    .iter()
                    .map(|option| self.compile(option, Some(&value.kind)))
                    .collect::<Result<Vec<_>>>()?;
                for option in &options {
                    common_type(&value.kind, &option.kind)?;
                }
                make("boolean", Node::In(boxed(value), options, *negated))
            }
            Expr::Cast {
                expr,
                data_type,
                format,
                ..
            } if format.is_none() => {
                let kind = crate::sql::kind(data_type)?;
                let hint = if unknown(expr) { Some(kind) } else { None };
                make(kind, Node::Cast(boxed(self.compile(expr, hint)?)))
            }
            Expr::Function(function) => {
                if function.over.is_some()
                    || function.filter.is_some()
                    || !function.within_group.is_empty()
                    || function.null_treatment.is_some()
                    || function.parameters != FunctionArguments::None
                {
                    return Err(unsupported());
                }
                let name = function.name.to_string().to_ascii_lowercase();
                let FunctionArguments::List(arguments) = &function.args else {
                    return Err(unsupported());
                };
                if !arguments.clauses.is_empty() {
                    return Err(unsupported());
                }
                let distinct = arguments.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct);
                let aggregate = ["count", "min", "max"].contains(&name.as_str());
                if aggregate {
                    if arguments.args.len() != 1 {
                        return Err(unsupported());
                    }
                    let input = match &arguments.args[0] {
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if name == "count" && !distinct => None,
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => {
                            Some(boxed(self.compile(expression, None)?))
                        }
                        _ => return Err(unsupported()),
                    };
                    if input.as_ref().is_some_and(|input| input.is_aggregate()) {
                        return Err(DbError::new("42803", "nested aggregates are not allowed"));
                    }
                    let kind = if name == "count" {
                        "bigint".into()
                    } else {
                        input.as_ref().ok_or_else(unsupported)?.kind.clone()
                    };
                    if name != "count" && kind == "boolean" {
                        return Err(DbError::new("42883", "MIN/MAX are not defined for BOOLEAN"));
                    }
                    make(&kind, Node::Aggregate(name.clone(), input, distinct))
                } else {
                    if distinct
                        || !["lower", "upper", "length", "char_length", "coalesce", "abs"].contains(&name.as_str())
                    {
                        return Err(unsupported());
                    }
                    let mut common: Option<String> = None;
                    if name == "coalesce" {
                        for argument in &arguments.args {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) = argument {
                                if !unknown(expression) {
                                    let kind = self.compile(expression, None)?.kind;
                                    common = Some(if let Some(previous) = common {
                                        common_type(&previous, &kind)?
                                    } else {
                                        kind
                                    });
                                }
                            }
                        }
                    }
                    let mut inputs = Vec::new();
                    for argument in &arguments.args {
                        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) = argument else {
                            return Err(unsupported());
                        };
                        let hint = match name.as_str() {
                            "lower" | "upper" | "length" | "char_length" => Some("text"),
                            "abs" => None,
                            _ => common
                                .as_deref()
                                .or_else(|| inputs.first().map(|input: &Expression| input.kind.as_str())),
                        };
                        let input = self.compile(expression, hint)?;
                        if let Some(hint) = hint {
                            common_type(&input.kind, hint)?;
                        }
                        if name == "abs" && !integral(&input.kind) {
                            return Err(mismatch());
                        }
                        inputs.push(input);
                    }
                    if inputs.is_empty() || (name != "coalesce" && inputs.len() != 1) {
                        return Err(unsupported());
                    }
                    let kind = match name.as_str() {
                        "length" | "char_length" => "integer".into(),
                        "coalesce" => common.unwrap_or_else(|| inputs[0].kind.clone()),
                        _ => inputs[0].kind.clone(),
                    };
                    make(&kind, Node::Function(name.clone(), inputs))
                }
            }
            _ => return Err(unsupported()),
        })
    }
}

pub fn truth(value: &Value) -> Result<Option<bool>> {
    if value.is_nil() {
        Ok(None)
    } else {
        value.as_bool().map(Some).ok_or_else(mismatch)
    }
}
pub fn compare(left: &Value, right: &Value) -> Result<Ordering> {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return Ok(left.cmp(&right));
    }
    if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        return Ok(left.cmp(right));
    }
    if let (Some(left), Some(right)) = (left.as_bool(), right.as_bool()) {
        return Ok(left.cmp(&right));
    }
    Err(mismatch())
}
fn nullable_bool(value: Option<bool>) -> Value {
    value.map(Value::from).unwrap_or(Value::Nil)
}
pub fn key(row: &[Value]) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    rmpv::encode::write_value(&mut result, &Value::Array(row.to_vec()))
        .map_err(|e| DbError::new("XX000", e.to_string()))?;
    Ok(result)
}

impl Expression {
    pub(crate) fn column_increment(&self, index: usize) -> Option<&Expression> {
        if let Node::Binary(Binary::Plus, left, right) = &self.node {
            if matches!(left.node, Node::Column(column) if column == index)
                && matches!(right.node, Node::Literal(_) | Node::Parameter(_))
                && integral(&self.kind)
                && self.kind == left.kind
                && integral(&right.kind)
            {
                return Some(right);
            }
        }
        None
    }
    pub fn column_reference(index: usize, column: &Column) -> Self {
        Self {
            kind: column.kind.clone(),
            node: Node::Column(index),
        }
    }
    pub fn equi_columns(&self) -> Option<(usize, usize)> {
        if let Node::Binary(Binary::Eq, left, right) = &self.node {
            if let (Node::Column(a), Node::Column(b)) = (&left.node, &right.node) {
                return Some((*a, *b));
            }
        }
        if let Node::Binary(Binary::And, left, right) = &self.node {
            return left.equi_columns().or_else(|| right.equi_columns());
        }
        None
    }
    pub fn equality_value(&self, column: usize) -> Option<Self> {
        if let Some(value) = self.exact_equality_value(column) {
            return Some(value.clone());
        }
        if let Node::Binary(Binary::And, left, right) = &self.node {
            return left.equality_value(column).or_else(|| right.equality_value(column));
        }
        None
    }
    pub(crate) fn exact_equality_value(&self, column: usize) -> Option<&Expression> {
        if let Node::Binary(Binary::Eq, left, right) = &self.node {
            if left.node == Node::Column(column) && right.is_constant() {
                return Some(right);
            }
            if right.node == Node::Column(column) && left.is_constant() {
                return Some(left);
            }
        }
        None
    }
    pub fn is_constant(&self) -> bool {
        match &self.node {
            Node::Column(_) | Node::Aggregate(..) => false,
            Node::Binary(_, a, b) => a.is_constant() && b.is_constant(),
            Node::Unary(_, input) | Node::IsNull(input, _) | Node::Cast(input) => input.is_constant(),
            Node::In(input, values, _) => input.is_constant() && values.iter().all(Self::is_constant),
            Node::Function(_, values) => values.iter().all(Self::is_constant),
            _ => true,
        }
    }
    pub fn column(&self, name: String) -> Column {
        Column {
            name,
            kind: self.kind.clone(),
            nullable: true,
        }
    }
    pub fn is_aggregate(&self) -> bool {
        match &self.node {
            Node::Aggregate(..) => true,
            Node::Binary(_, a, b) => a.is_aggregate() || b.is_aggregate(),
            Node::Unary(_, input) | Node::IsNull(input, _) | Node::Cast(input) => input.is_aggregate(),
            Node::In(input, values, _) => input.is_aggregate() || values.iter().any(Self::is_aggregate),
            Node::Function(_, values) => values.iter().any(Self::is_aggregate),
            _ => false,
        }
    }
    pub fn valid_group(&self, groups: &[Self]) -> bool {
        if groups.contains(self) {
            return true;
        }
        match &self.node {
            Node::Column(_) => false,
            Node::Binary(_, a, b) => a.valid_group(groups) && b.valid_group(groups),
            Node::Unary(_, input) | Node::IsNull(input, _) | Node::Cast(input) => input.valid_group(groups),
            Node::In(input, values, _) => input.valid_group(groups) && values.iter().all(|v| v.valid_group(groups)),
            Node::Function(_, values) => values.iter().all(|v| v.valid_group(groups)),
            _ => true,
        }
    }
    pub fn eval(&self, row: &[Value], parameters: &[Value], group: Option<&[Row]>) -> Result<Value> {
        let eval = |expression: &Self| expression.eval(row, parameters, group);
        Ok(match &self.node {
            Node::Column(index) => row
                .get(*index)
                .cloned()
                .ok_or_else(|| DbError::new("XX000", "row width differs from plan"))?,
            Node::Literal(value) => value.clone(),
            Node::Parameter(index) => parameters
                .get(*index)
                .cloned()
                .ok_or_else(|| DbError::new("08P01", "missing parameter"))?,
            Node::IsNull(input, negated) => (eval(input)?.is_nil() != *negated).into(),
            Node::Cast(input) => {
                let value = eval(input)?;
                let text = match &value {
                    Value::Nil => None,
                    Value::String(text) => text.as_str().map(str::to_owned),
                    Value::Boolean(value) => Some(value.to_string()),
                    _ => Some(value.to_string()),
                };
                text_value(text.as_deref(), &self.column(String::new()))?
            }
            Node::Unary(op, input) => {
                let value = eval(input)?;
                if value.is_nil() {
                    Value::Nil
                } else {
                    match op {
                        Unary::Not => (!value.as_bool().ok_or_else(mismatch)?).into(),
                        Unary::Minus => integer_value(
                            value
                                .as_i64()
                                .ok_or_else(mismatch)?
                                .checked_neg()
                                .ok_or_else(overflow)?,
                            &self.kind,
                        )?,
                        Unary::Plus => value,
                        _ => return Err(unsupported()),
                    }
                }
            }
            Node::Binary(op, left, right) => {
                let left = eval(left)?;
                if *op == Binary::And && truth(&left)? == Some(false) {
                    return Ok(false.into());
                }
                if *op == Binary::Or && truth(&left)? == Some(true) {
                    return Ok(true.into());
                }
                let right = eval(right)?;
                if matches!(op, Binary::And | Binary::Or) {
                    let (a, b) = (truth(&left)?, truth(&right)?);
                    nullable_bool(if *op == Binary::And {
                        if a == Some(false) || b == Some(false) {
                            Some(false)
                        } else if a.is_none() || b.is_none() {
                            None
                        } else {
                            Some(true)
                        }
                    } else if a == Some(true) || b == Some(true) {
                        Some(true)
                    } else if a.is_none() || b.is_none() {
                        None
                    } else {
                        Some(false)
                    })
                } else if left.is_nil() || right.is_nil() {
                    Value::Nil
                } else {
                    match op {
                        Binary::Eq => (compare(&left, &right)? == Ordering::Equal).into(),
                        Binary::NotEq => (compare(&left, &right)? != Ordering::Equal).into(),
                        Binary::Lt => compare(&left, &right)?.is_lt().into(),
                        Binary::LtEq => (!compare(&left, &right)?.is_gt()).into(),
                        Binary::Gt => compare(&left, &right)?.is_gt().into(),
                        Binary::GtEq => (!compare(&left, &right)?.is_lt()).into(),
                        Binary::StringConcat => format!(
                            "{}{}",
                            left.as_str().ok_or_else(mismatch)?,
                            right.as_str().ok_or_else(mismatch)?
                        )
                        .into(),
                        _ => {
                            let (a, b) = (
                                left.as_i64().ok_or_else(mismatch)?,
                                right.as_i64().ok_or_else(mismatch)?,
                            );
                            let result = match op {
                                Binary::Plus => core::checked_add_i64(a, b),
                                Binary::Minus => core::checked_sub_i64(a, b),
                                Binary::Multiply => a.checked_mul(b),
                                Binary::Divide | Binary::Modulo if b == 0 => {
                                    return Err(DbError::new("22012", "division by zero"))
                                }
                                Binary::Divide => a.checked_div(b),
                                Binary::Modulo if b == -1 => Some(0),
                                Binary::Modulo => a.checked_rem(b),
                                _ => return Err(unsupported()),
                            };
                            integer_value(result.ok_or_else(overflow)?, &self.kind)?
                        }
                    }
                }
            }
            Node::In(input, options, negated) => {
                let value = eval(input)?;
                let mut unknown = value.is_nil();
                let mut matched = false;
                for option in options {
                    let candidate = eval(option)?;
                    if candidate.is_nil() {
                        unknown = true;
                    } else if !value.is_nil() && compare(&value, &candidate)? == Ordering::Equal {
                        matched = true;
                        break;
                    }
                }
                nullable_bool(if matched {
                    Some(!negated)
                } else if unknown {
                    None
                } else {
                    Some(*negated)
                })
            }
            Node::Function(name, inputs) => {
                if name == "coalesce" {
                    for input in inputs {
                        let value = eval(input)?;
                        if !value.is_nil() {
                            return Ok(value);
                        }
                    }
                    Value::Nil
                } else {
                    let value = eval(&inputs[0])?;
                    if value.is_nil() {
                        Value::Nil
                    } else {
                        match name.as_str() {
                            "lower" => value.as_str().ok_or_else(mismatch)?.to_lowercase().into(),
                            "upper" => value.as_str().ok_or_else(mismatch)?.to_uppercase().into(),
                            "length" | "char_length" => {
                                (value.as_str().ok_or_else(mismatch)?.chars().count() as i64).into()
                            }
                            "abs" => integer_value(
                                value
                                    .as_i64()
                                    .ok_or_else(mismatch)?
                                    .checked_abs()
                                    .ok_or_else(overflow)?,
                                &self.kind,
                            )?,
                            _ => return Err(unsupported()),
                        }
                    }
                }
            }
            Node::Aggregate(name, input, distinct) => {
                let group = group.ok_or_else(|| DbError::new("42803", "aggregate is not valid here"))?;
                let mut values = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for row in group {
                    let value = if let Some(input) = input {
                        input.eval(row, parameters, None)?
                    } else {
                        1.into()
                    };
                    if value.is_nil() || (*distinct && !seen.insert(key(std::slice::from_ref(&value))?)) {
                        continue;
                    }
                    values.push(value);
                }
                if name == "count" {
                    (values.len() as i64).into()
                } else {
                    let mut best: Option<Value> = None;
                    for value in values {
                        let better = best
                            .as_ref()
                            .map(|best| {
                                compare(&value, best).map(
                                    |order| {
                                        if name == "min" {
                                            order.is_lt()
                                        } else {
                                            order.is_gt()
                                        }
                                    },
                                )
                            })
                            .transpose()?
                            .unwrap_or(true);
                        if better {
                            best = Some(value);
                        }
                    }
                    best.unwrap_or(Value::Nil)
                }
            }
        })
    }
}
