use std::io::{self, BufRead, Write};
use libsql_graph::prelude::*;
use libsql_graph::cypher::executor::Value;

fn value_to_json(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Node(id) => format!("{{\"_node\":{id}}}"),
        Value::Rel(id) => format!("{{\"_rel\":{id}}}"),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(value_to_json).collect();
            format!("[{}]", inner.join(","))
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: graph_cli <db_path> [cypher_query]");
        eprintln!("  With query: execute single query, print JSON, exit");
        eprintln!("  Without query: REPL mode (one query per line from stdin)");
        std::process::exit(1);
    }

    let db_path = &args[1];

    let mut engine = if std::path::Path::new(db_path).exists() {
        GraphEngine::open(db_path).unwrap_or_else(|e| {
            eprintln!("{{\"error\":\"{e}\"}}");
            std::process::exit(1);
        })
    } else {
        GraphEngine::create(db_path, 4096).unwrap_or_else(|e| {
            eprintln!("{{\"error\":\"{e}\"}}");
            std::process::exit(1);
        })
    };

    if args.len() >= 3 {
        let cypher = args[2..].join(" ");
        run_query(&mut engine, &cypher);
        return;
    }

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("!rel ") {
            run_create_rel(&mut engine, rest);
        } else if let Some(rest) = trimmed.strip_prefix("!prop ") {
            run_set_prop(&mut engine, rest);
        } else {
            run_query(&mut engine, trimmed);
        }
        let _ = stdout.flush();
    }
}

fn run_create_rel(engine: &mut GraphEngine<libsql_graph::storage::pager_bridge::FilePager>, args: &str) {
    let parts: Vec<&str> = args.splitn(3, ' ').collect();
    if parts.len() < 3 {
        println!("{{\"error\":\"usage: !rel <src_id> <dst_id> <type>\"}}");
        return;
    }
    let src: u64 = match parts[0].parse() { Ok(v) => v, Err(_) => { println!("{{\"error\":\"invalid src_id\"}}"); return; } };
    let dst: u64 = match parts[1].parse() { Ok(v) => v, Err(_) => { println!("{{\"error\":\"invalid dst_id\"}}"); return; } };
    match engine.create_relationship(src, dst, parts[2]) {
        Ok(rel_id) => println!("{{\"rel_id\":{rel_id}}}"),
        Err(e) => println!("{{\"error\":\"{e}\"}}"),
    }
}

fn run_set_prop(engine: &mut GraphEngine<libsql_graph::storage::pager_bridge::FilePager>, args: &str) {
    let parts: Vec<&str> = args.splitn(4, ' ').collect();
    if parts.len() < 4 {
        println!("{{\"error\":\"usage: !prop node|rel <id> <key> <value>\"}}");
        return;
    }
    let id: u64 = match parts[1].parse() { Ok(v) => v, Err(_) => { println!("{{\"error\":\"invalid id\"}}"); return; } };
    let value = libsql_graph::PropertyValue::ShortString(parts[3].to_string());
    let result = if parts[0] == "node" {
        engine.set_node_property(id, parts[2], value)
    } else {
        engine.set_rel_property(id, parts[2], value)
    };
    match result {
        Ok(()) => println!("{{\"ok\":true}}"),
        Err(e) => println!("{{\"error\":\"{e}\"}}"),
    }
}

fn run_query(engine: &mut GraphEngine<libsql_graph::storage::pager_bridge::FilePager>, cypher: &str) {
    match engine.query(cypher) {
        Ok(result) => {
            let rows_json: Vec<String> = result.rows.iter().map(|row| {
                let cells: Vec<String> = row.iter().map(value_to_json).collect();
                format!("[{}]", cells.join(","))
            }).collect();
            let cols_json: Vec<String> = result.columns.iter().map(|c| format!("\"{c}\"")).collect();
            println!("{{\"columns\":[{}],\"rows\":[{}],\"stats\":{{\"nodes_created\":{},\"relationships_created\":{},\"properties_set\":{},\"nodes_deleted\":{}}}}}",
                cols_json.join(","),
                rows_json.join(","),
                result.stats.nodes_created,
                result.stats.relationships_created,
                result.stats.properties_set,
                result.stats.nodes_deleted,
            );
        }
        Err(e) => {
            println!("{{\"error\":\"{}\"}}", format!("{e}").replace('"', "\\\""));
        }
    }
}
