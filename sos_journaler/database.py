from influxdb import InfluxDBClient

from sos_journaler import config


class FIXMDatabase(InfluxDBClient):
    DEFAULT_RETENTION_POLICY_NAME = "hard_cutoff"

    def __init__(self):
        super().__init__(config.influxdb_hostname, config.influxdb_port,
                         config.influxdb_username, config.influxdb_password,
                         config.influxdb_database)

        self._init_database_config()

    def _init_database_config(self) -> None:
        # Create the database if it doesn't already exist
        self.create_database(config.influxdb_database)

        # Check if any retention policies exist with our name
        if all(policy["name"] != self.DEFAULT_RETENTION_POLICY_NAME
               for policy in self.get_list_retention_policies()):
            # Configure the retention policy
            self.create_retention_policy(
                name=self.DEFAULT_RETENTION_POLICY_NAME,
                duration=config.influxdb_retention_duration,
                replication="1",
                default=True
            )
