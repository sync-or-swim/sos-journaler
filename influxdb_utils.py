class Point:
    """A class version of an InfluxDB point."""
    def __init__(self, measurement, time):
        """
        :param measurement: Which measurement (table in SQL terms) this should
            be saved to
        :param time: The time this data took place at
        """
        self.measurement = measurement
        self.time = time
        self.tags = {}
        self.fields = {}

    def to_dict(self):
        """Converts to a format that InfluxDB can save."""
        return {
            "measurement": self.measurement,
            "tags": self.tags,
            "time": self.time,
            "fields": self.fields,
        }

    def check_types(self):
        """Checks to make sure all fields and tags are of a format InfluxDB can
        save.
        """
        all_items = list(self.fields.items()) + list(self.tags.items())
        for name, val in all_items:
            if type(val) not in [str, float, int]:
                raise ValueError(f"Item {name} is of invalid type {type(val)}")
