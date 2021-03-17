use data_panel_common::service::Service;
use data_panel_database::service::mysql::MySQLService;

pub fn new_service() -> Box<dyn Service> {
    Box::new(MySQLService {})
}