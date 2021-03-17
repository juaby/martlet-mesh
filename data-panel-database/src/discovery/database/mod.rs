use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    name: String,
    segments: Segments,
    dis_rules: DisRules,
}

impl Cluster {
    pub fn get_name(&self) -> &String {
        &self.name
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Segments {
    meta_segment: MetaSegment,
    data_segments: HashMap<u32, DataSegment>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MetaSegment {
    primary: Segment,
    mirrors: Vec<Segment>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DataSegment {
    primary: Segment,
    mirrors: Vec<Segment>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    id: u32,
    url: String,
    username: String,
    password: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DisRules {
    distributed_tables: HashMap<String, DisTable>,
    replicated_tables: Vec<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DisTable {
    dis_keys: Vec<String>,
    dis_algorithm: DisAlgorithm,
    dis_relatives: Vec<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DisAlgorithm {
    dis_type: DisType,
    dis_expression: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DisType {
    HASH,
    RANGE,
    CUSTOM,
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Read;

    use rhai::{Engine, Scope};

    use crate::discovery::database::{Cluster, DataSegment, DisAlgorithm, DisRules, DisTable, DisType, MetaSegment, Segment, Segments};

    #[test]
    fn test_custom_route() {
        let engine = Engine::new();

        // First create the state
        let mut scope = Scope::new();

        // Compile to an AST and store it for later evaluations
        let ast = engine.compile("x + y").unwrap();

        scope.push("x", 20);
        scope.push("y", 30);

        for i in 0..42 {
            scope.set_value("x", i);
            scope.set_value("y", i);
            let result: i32 = engine.eval_ast_with_scope(&mut scope, &ast).unwrap();

            println!("Answer #{}: {}", i, result);      // prints double i
        }
    }

    #[test]
    fn test_yaml_from_file() {
        let mut file = File::open("./etc/dbmesh.yaml").expect("Unable to open file");
        let mut contents = String::new();

        file.read_to_string(&mut contents)
            .expect("Unable to read file");

        println!("{}", contents);
        let deserialized_rc: Cluster = serde_yaml::from_str(&contents).unwrap();
        println!("{:#?}", deserialized_rc);
    }

    #[test]
    fn test_yaml() {
        let mut data_segments = HashMap::new();
        data_segments.insert(100, DataSegment {
            primary: Segment {
                id: 0,
                url: String::from("jdbc:mysql://localhost:3306/martlet"),
                username: String::from("root"),
                password: String::from("root"),
            },
            mirrors: vec![
                Segment {
                    id: 0,
                    url: String::from("jdbc:mysql://localhost:3306/martlet"),
                    username: String::from("root"),
                    password: String::from("root"),
                },
                Segment {
                    id: 1,
                    url: String::from("jdbc:mysql://localhost:3306/martlet"),
                    username: String::from("root"),
                    password: String::from("root"),
                }
            ],
        });
        data_segments.insert(200, DataSegment {
            primary: Segment {
                id: 0,
                url: String::from("jdbc:mysql://localhost:3306/martlet"),
                username: String::from("root"),
                password: String::from("root"),
            },
            mirrors: vec![
                Segment {
                    id: 1,
                    url: String::from("jdbc:mysql://localhost:3306/martlet"),
                    username: String::from("root"),
                    password: String::from("root"),
                },
                Segment {
                    id: 2,
                    url: String::from("jdbc:mysql://localhost:3306/martlet"),
                    username: String::from("root"),
                    password: String::from("root"),
                }
            ],
        });
        data_segments.insert(300, DataSegment {
            primary: Segment {
                id: 0,
                url: String::from("jdbc:mysql://localhost:3306/martlet"),
                username: String::from("root"),
                password: String::from("root"),
            },
            mirrors: vec![
                Segment {
                    id: 0,
                    url: String::from("jdbc:mysql://localhost:3306/martlet"),
                    username: String::from("root"),
                    password: String::from("root"),
                },
                Segment {
                    id: 1,
                    url: String::from("jdbc:mysql://localhost:3306/martlet"),
                    username: String::from("root"),
                    password: String::from("root"),
                }
            ],
        });
        let mut distributed_tables = HashMap::new();
        distributed_tables.insert(String::from("t_order"), DisTable {
            dis_keys: vec![String::from("user_id")],
            dis_relatives: vec![String::from("t_order_item")],
            dis_algorithm: DisAlgorithm {
                dis_type: DisType::HASH,
                dis_expression: String::from("x + y / 3"),
            },
        });
        distributed_tables.insert(String::from("t_order_item"), DisTable {
            dis_keys: vec![],
            dis_relatives: vec![],
            dis_algorithm: DisAlgorithm {
                dis_type: DisType::HASH,
                dis_expression: String::from("x + y / 3"),
            },
        });
        let rc = Cluster {
            name: String::from("martlet"),
            segments: Segments {
                meta_segment: MetaSegment {
                    primary: Segment {
                        id: 0,
                        url: String::from("jdbc:mysql://localhost:3306/martlet"),
                        username: String::from("root"),
                        password: String::from("root"),
                    },
                    mirrors: vec![
                        Segment {
                            id: 0,
                            url: String::from("jdbc:mysql://localhost:3306/martlet"),
                            username: String::from("root"),
                            password: String::from("root"),
                        },
                        Segment {
                            id: 1,
                            url: String::from("jdbc:mysql://localhost:3306/martlet"),
                            username: String::from("root"),
                            password: String::from("root"),
                        }
                    ],
                },
                data_segments: data_segments,
            },
            dis_rules: DisRules {
                distributed_tables,
                replicated_tables: vec![String::from("t_dept"), String::from("t_root")],
            },
        };
        let s = serde_yaml::to_string(&rc).unwrap();
        println!("{}", s);
        let deserialized_rc: Cluster = serde_yaml::from_str(&s).unwrap();
        println!("{:#?}", deserialized_rc);
    }
}
