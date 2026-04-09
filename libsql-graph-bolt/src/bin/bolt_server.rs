use libsql_graph_bolt::server::BoltServer;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut db_path = String::from("./graph.db");
    let mut port = String::from("7687");

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--db" => {
                i += 1;
                if i < args.len() {
                    db_path = args[i].clone();
                }
            }
            "--port" => {
                i += 1;
                if i < args.len() {
                    port = args[i].clone();
                }
            }
            _ => {}
        }
        i += 1;
    }

    let addr = format!("0.0.0.0:{}", port);
    eprintln!("Bolt server listening on {}", addr);
    eprintln!("Database: {}", db_path);

    let server = BoltServer::bind(&addr, &db_path).await.unwrap();
    server.run().await.unwrap();
}
