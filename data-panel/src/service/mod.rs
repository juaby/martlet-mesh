use data_panel_database::service::mysql::MySQLService;
use data_panel_common::service::Service;

pub fn new_service() -> Box<dyn Service> {
    Box::new(MySQLService {})
}