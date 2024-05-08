use anyhow::Result;
use petgraph::dot::Dot;
use petgraph::graph::DiGraph;
use serde::Deserialize;
use serde::Serialize;
use sqlparser::ast::Expr;
use sqlparser::ast::SelectItem;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::mem;
use std::path::Path;
use std::path::PathBuf;

// Had some issues with how petgraph parsed newline so
// circumventing it by using this symbol and replacing it.
const NEWLINE_PLACEHOLDER: &'static str = "_||_";

const OUTPUT_PATH: &'static str = "graph.dot";

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!(
            "Usage: {} <path to sources.yml> <path to folder with .sql files>",
            args[0]
        );
        std::process::exit(1);
    }

    let sources_path = PathBuf::from(&args[1]);
    let sql_dir = PathBuf::from(&args[2]);

    let tables = Tables::from_path(&sources_path)?;
    let models = SqlModels::load_from_dir(&sql_dir, &tables)?;

    let graph = models.to_graph();
    let output = PathBuf::from(OUTPUT_PATH);
    write_graph(graph, &output)?;

    Ok(())
}

fn write_graph(graph: DiGraph<String, String>, path: &Path) -> Result<()> {
    let s = format!("{:?}", Dot::new(&graph)).replace(NEWLINE_PLACEHOLDER, "\n");
    let mut file = fs::File::create(path)?;
    file.write_all(s.as_bytes())?;
    eprintln!("graph successfully written to file: {}", OUTPUT_PATH);
    Ok(())
}

fn extract_items_and_tables(
    statement: &Statement,
    source_tables: &mut Vec<SourceTable>,
    items: &mut Vec<Item>,
) {
    let Statement::Query(query) = statement else {
        return;
    };

    let SetExpr::Select(ref select) = *query.body else {
        return;
    };

    for item in &select.projection {
        if let SelectItem::ExprWithAlias { expr, alias } = item {
            let expr = match expr {
                Expr::IsNotNull(expr) => expr,
                expr => expr,
            };

            match expr {
                Expr::CompoundIdentifier(ref idents) => {
                    let mut path: Vec<String> = idents
                        .clone()
                        .iter()
                        .map(|ident| ident.value.clone())
                        .collect();
                    let name = path.pop().unwrap();
                    let item = Item {
                        name,
                        path,
                        alias: Some(alias.value.clone()),
                        data_type: None,
                    };

                    items.push(item);
                }

                _ => {}
            }
        }

        let SelectItem::UnnamedExpr(expr) = item else {
            continue;
        };

        match expr {
            Expr::Identifier(ident) => {
                let item = Item {
                    path: vec![],
                    name: ident.value.clone(),
                    alias: None,
                    data_type: None,
                };

                items.push(item);
            }
            Expr::CompoundIdentifier(ref idents) => {
                let mut path: Vec<String> = idents
                    .clone()
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect();
                let name = path.pop().unwrap();
                let item = Item {
                    name,
                    path,
                    alias: None,
                    data_type: None,
                };

                items.push(item);
            }
            _ => {}
        };
    }

    for table_with_join in &select.from {
        if let TableFactor::Table { name, alias, .. } = &table_with_join.relation {
            let table = SourceTable {
                origin: name.0.iter().map(|id| id.value.clone()).collect(),
                alias: alias.clone().map(|alias| alias.name.value),
            };

            source_tables.push(table);
        }
        for join in &table_with_join.joins {
            if let TableFactor::Table { name, alias, .. } = &join.relation {
                let table = SourceTable {
                    origin: name.0.iter().map(|id| id.value.clone()).collect(),
                    alias: alias.clone().map(|alias| alias.name.value),
                };

                source_tables.push(table);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Item {
    name: String,
    path: Vec<String>,
    alias: Option<String>,
    data_type: Option<DataType>,
}

impl Item {
    fn name(&self) -> &str {
        if let Some(alias) = &self.alias {
            return alias.as_str();
        }

        &self.name
    }

    fn filter_by_source(items: &mut Vec<Self>, source: SourceTable) {
        items.retain(|item| {
            if let Some(alias) = &source.alias {
                if alias == &item.path[0] {
                    return true;
                }
            }

            if &source.origin == &item.path {
                return true;
            }

            false
        });
    }

    fn resolve_path(&self, tables: &Vec<SourceTable>) -> Vec<String> {
        if self.path.len() == 0 {
            if tables.len() == 1 {
                return tables[0].origin.clone();
            } else {
                panic!("ambigious path");
            }
        }

        if self.path.len() > 1 {
            return self.path.clone();
        }

        for table in tables {
            if let Some(alias) = &table.alias {
                if alias == &self.path[0] {
                    return table.origin.clone();
                }
            }
        }

        vec![]
    }

    fn find(items: &Vec<Item>, name: &str) -> Option<Item> {
        for item in items {
            if let Some(alias) = &item.alias {
                if alias == name {
                    let item = item.clone();
                    return Some(item);
                }
            }
        }

        for item in items {
            if &item.name == name {
                return Some(item.clone());
            }
        }

        None
    }
}

#[derive(Clone, Debug)]
struct SourceTable {
    origin: Vec<String>,
    alias: Option<String>,
}

#[derive(Clone, Debug)]
struct SqlModel {
    name: String,
    tables: Vec<SourceTable>,
    items: Vec<Item>,
}

impl SqlModel {
    fn from_path(path: &Path) -> Result<Self> {
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        let str = fs::read_to_string(&path)?;
        let statements = Parser::parse_sql(&GenericDialect, &str)?;
        let mut tables = vec![];
        let mut items = vec![];
        for statement in &statements {
            extract_items_and_tables(statement, &mut tables, &mut items);
        }
        Ok(Self {
            name,
            tables,
            items,
        })
    }

    fn assign_datatypes(&mut self, tables: &Tables, others: &Vec<Self>) {
        for item in &mut self.items {
            let source = item
                .resolve_path(&self.tables)
                .iter()
                .last()
                .unwrap()
                .to_owned();

            if let Some(ty) = tables.data_type(&item.resolve_path(&self.tables), &item.name) {
                item.data_type = Some(ty);
                continue;
            }

            for other in others {
                if other.name == source {
                    let rec_item = Item::find(&other.items, &item.name);
                    if let Some(ref rec_item) = rec_item {
                        let x = rec_item.resolve_path(&other.tables);
                        if let Some(ty) = tables.data_type(&x, &rec_item.name) {
                            item.data_type = Some(ty);
                        }
                    }
                }
            }
        }
    }
}

fn find_sql_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut sql_files = Vec::new();

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            // Check if it's a file and ends with `.sql`
            if path.is_file() && path.extension().and_then(std::ffi::OsStr::to_str) == Some("sql") {
                sql_files.push(path);
            }
        }
    } else {
        return Err(
            std::io::Error::new(std::io::ErrorKind::NotFound, "Directory not found").into(),
        );
    }

    Ok(sql_files)
}

/// A collection of sqlmodels that reference each other.
struct SqlModels(Vec<SqlModel>);

impl SqlModels {
    fn load_from_dir(dir: &Path, tables: &Tables) -> Result<Self> {
        let sql_paths = find_sql_files(&dir)?;
        let mut models = vec![];

        for path in sql_paths {
            models.push(SqlModel::from_path(&path)?);
        }

        let models_ref = models.clone();

        for model in &mut models {
            model.assign_datatypes(tables, &models_ref);
        }

        Ok(Self(models))
    }

    fn to_graph(&self) -> DiGraph<String, String> {
        let mut edgemap = HashMap::new();

        let mut edges = vec![];
        for model in &self.0 {
            for source in &model.tables {
                let to = model.name.clone();
                let mut items = model.items.clone();
                Item::filter_by_source(&mut items, source.clone());
                let mut label = String::new();
                let last_index = items.len().saturating_sub(1);
                for (idx, item) in items.iter().enumerate() {
                    label.push_str(&format!(
                        "{} - {:?}",
                        item.name(),
                        item.data_type.unwrap_or_default()
                    ));

                    if idx != last_index {
                        label.push_str(NEWLINE_PLACEHOLDER);
                    }
                }

                let from = source.origin.last().unwrap().clone();

                edgemap.insert((from.clone(), to.clone()), mem::take(&mut label));
                edges.push((from, to));
            }
        }

        let mut graph = DiGraph::<String, String>::new();
        let mut inserted_edges = HashMap::new();

        for (from, to) in edges {
            let from_index = match inserted_edges.get(&from) {
                Some(idx) => *idx,
                None => {
                    let idx = graph.add_node(from.clone());
                    inserted_edges.insert(from.clone(), idx);
                    idx
                }
            };

            let to_index = match inserted_edges.get(&to) {
                Some(idx) => *idx,
                None => {
                    let idx = graph.add_node(to.clone());
                    inserted_edges.insert(to.clone(), idx);
                    idx
                }
            };

            graph.add_edge(
                from_index,
                to_index,
                edgemap.get(&(from, to)).unwrap().to_string(),
            );
        }

        graph
    }
}

/// Represents the sourcetables from `sources.yml`.
#[derive(Debug)]
struct Tables(Vec<Table>);

impl Tables {
    fn from_path(path: &Path) -> Result<Self> {
        let s = fs::read_to_string(path)?;
        let tables: Vec<Table> = serde_yaml::from_str(&s)?;
        Ok(Self(tables))
    }

    fn data_type(&self, path: &Vec<String>, column: &str) -> Option<DataType> {
        let table = self.0.iter().find(|tbl| tbl.path_matches(path))?;
        table
            .datafields
            .iter()
            .find(|dfl| dfl.name == column)
            .map(|dfl| dfl.datatype.r#type)
            .unwrap_or_default()
            .into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Table {
    name: String,
    namespace: Vec<String>,
    description: String,
    datafields: Vec<DataField>,
}

impl Table {
    fn path_matches(&self, path: &Vec<String>) -> bool {
        let mut full = self.namespace.clone();
        full.push(self.name.clone());

        &full == path
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy)]
enum DataType {
    String,
    DateTime,
    Object,
    Array,
    #[default]
    Unknown,
}

fn deserialize_data_type<'de, D>(deserializer: D) -> Result<DataType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    DataType::from_str(&s)
        .ok_or_else(|| serde::de::Error::custom(format!("invalid data type: {}", s)))
}

impl DataType {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "string" => Some(Self::String),
            "datetime" => Some(Self::DateTime),
            "object" => Some(Self::Object),
            "array" => Some(Self::Array),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    #[serde(deserialize_with = "deserialize_data_type")]
    r#type: DataType,
}

#[derive(Debug, Serialize, Deserialize)]
struct DataField {
    name: String,
    datatype: Metadata,
}
