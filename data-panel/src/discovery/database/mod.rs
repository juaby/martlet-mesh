use serde::{Serialize, Deserialize};
use std::borrow::Cow;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RouteContext<'a> {
    cluster_name: Cow<'a, str>,
    segments: Segments<'a>,
    dis_rules: DisRules<'a>
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Segments<'a> {
    meta_segment: MetaSegment<'a>,
    data_segments: HashMap<u32, DataSegment<'a>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MetaSegment<'a> {
    primary: Segment<'a>,
    mirrors: Vec<Segment<'a>>
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DataSegment<'a> {
    primary: Segment<'a>,
    mirrors: Vec<Segment<'a>>
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Segment<'a> {
    id: u32,
    url: Cow<'a, str>,
    username: Cow<'a, str>,
    password:Cow<'a, str>
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DisRules<'a> {
    distributed_rules: HashMap<Cow<'a, str>, DisTableInfo<'a>>,
    replicated_tables: Vec<Cow<'a, str>>
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DisTableInfo<'a> {
    dis_keys: Vec<Cow<'a, str>>,
    dis_rels: Vec<Cow<'a, str>>,
    dis_algorithm: DisAlgorithm<'a>,
    dis_rel_to: Cow<'a, str>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DisAlgorithm<'a> {
    dis_type: DisType,
    dis_expression: Cow<'a, str>
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DisType {
    HASH, RANGE, CUSTOM
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::discovery::database::{RouteContext, Segments, DisRules, MetaSegment, Segment, DataSegment, DisTableInfo, DisAlgorithm, DisType};
    use std::borrow::Cow;
    use std::fs::File;
    use std::io::Read;
    use rhai::{Engine, Scope};

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
        let deserialized_rc: RouteContext = serde_yaml::from_str(&contents).unwrap();
        println!("{:#?}", deserialized_rc);
    }

    #[test]
    fn test_yaml() {
        let mut data_segments = HashMap::new();
        data_segments.insert(100, DataSegment {
            primary: Segment {
                id: 0,
                url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                username: Cow::from("root"),
                password: Cow::from("root")
            },
            mirrors: vec![
                Segment {
                    id: 0,
                    url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                    username: Cow::from("root"),
                    password: Cow::from("root")
                },
                Segment {
                    id: 0,
                    url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                    username: Cow::from("root"),
                    password: Cow::from("root")
                }
            ]
        });
        data_segments.insert(200, DataSegment {
            primary: Segment {
                id: 0,
                url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                username: Cow::from("root"),
                password: Cow::from("root")
            },
            mirrors: vec![
                Segment {
                    id: 0,
                    url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                    username: Cow::from("root"),
                    password: Cow::from("root")
                },
                Segment {
                    id: 0,
                    url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                    username: Cow::from("root"),
                    password: Cow::from("root")
                }
            ]
        });
        data_segments.insert(300, DataSegment {
            primary: Segment {
                id: 0,
                url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                username: Cow::from("root"),
                password: Cow::from("root")
            },
            mirrors: vec![
                Segment {
                    id: 0,
                    url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                    username: Cow::from("root"),
                    password: Cow::from("root")
                },
                Segment {
                    id: 0,
                    url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                    username: Cow::from("root"),
                    password: Cow::from("root")
                }
            ]
        });
        let mut distributed_rules = HashMap::new();
        distributed_rules.insert(Cow::from("t_order"), DisTableInfo {
            dis_keys: vec![Cow::from("user_id")],
            dis_rels: vec![Cow::from("t_order_item")],
            dis_algorithm: DisAlgorithm {
                dis_type: DisType::HASH,
                dis_expression: Cow::from("x + y / 3")
            },
            dis_rel_to: Cow::from("")
        });
        distributed_rules.insert(Cow::from("t_order_item"), DisTableInfo {
            dis_keys: vec![],
            dis_rels: vec![],
            dis_algorithm: DisAlgorithm {
                dis_type: DisType::HASH,
                dis_expression: Cow::from("x + y / 3")
            },
            dis_rel_to: Cow::from("t_order")
        });
        let rc = RouteContext {
            cluster_name: Cow::from("martlet"),
            segments: Segments {
                meta_segment: MetaSegment {
                    primary: Segment {
                        id: 0,
                        url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                        username: Cow::from("root"),
                        password: Cow::from("root")
                    },
                    mirrors: vec![
                        Segment {
                            id: 0,
                            url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                            username: Cow::from("root"),
                            password: Cow::from("root")
                        },
                        Segment {
                            id: 0,
                            url: Cow::from("jdbc:mysql://localhost:3306/martlet"),
                            username: Cow::from("root"),
                            password: Cow::from("root")
                        }
                    ]
                },
                data_segments: data_segments
            },
            dis_rules: DisRules {
                distributed_rules: distributed_rules,
                replicated_tables: vec![Cow::from("t_dept"), Cow::from("t_root")]
            }
        };
        let s = serde_yaml::to_string(&rc).unwrap();
        println!("{}", s);
        let deserialized_rc: RouteContext = serde_yaml::from_str(&s).unwrap();
        println!("{:#?}", deserialized_rc);
    }
}
