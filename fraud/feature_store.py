class FeatureStore:
    def __init__(self, feature_configs):
        self.feature_configs = feature_configs
        self.profiles = {}

    def read_features(self, customer_id, current_time):
        """Read all features for a customer. Works for both bucketed and static types."""
        result = {}

        if customer_id not in self.profiles:
            for feature in self.feature_configs:
                result[feature["name"]] = feature.get("default", 0)
            return result

        profile = self.profiles[customer_id]

        for feature in self.feature_configs:
            name = feature["name"]
            ftype = feature["type"]

            if name not in profile:
                result[name] = feature.get("default", 0)
                continue

            # Static features — just return the stored value
            if ftype == "latest":
                result[name] = profile[name]
                continue

            # Bucketed features — aggregate within window
            window = feature["window"]
            cutoff = current_time - window
            buckets = profile[name]

            expired = [k for k in buckets if k < cutoff]
            for k in expired:
                del buckets[k]

            if ftype == "sum":
                result[name] = sum(buckets.values())
            elif ftype == "count":
                result[name] = sum(buckets.values())
            elif ftype == "unique":
                all_values = set()
                for s in buckets.values():
                    all_values = all_values.union(s)
                result[name] = len(all_values)

        return result

    def update(self, event):
        """
        Update features from any event (transaction, account opening, card issue).
        Each feature config has a 'source' field that filters which events it processes.
        """
        cid = event["customer_id"]
        event_source = event.get("_source", "transaction")

        if cid not in self.profiles:
            self.profiles[cid] = {}

        profile = self.profiles[cid]

        for feature in self.feature_configs:
            # Only process features that match this event's source
            if feature.get("source", "transaction") != event_source:
                continue

            # Check additional filters
            if "filter" in feature:
                skip = False
                for key, value in feature["filter"].items():
                    if event.get(key) != value:
                        skip = True
                        break
                if skip:
                    continue

            name = feature["name"]
            ftype = feature["type"]

            # Static — store the latest value
            if ftype == "latest":
                profile[name] = event[feature["field"]]
                continue

            # Bucketed — aggregate into time buckets
            timestamp = event["timestamp"]
            bucket_size = feature["bucket_size"]
            bucket_key = (timestamp // bucket_size) * bucket_size

            if name not in profile:
                profile[name] = {}

            buckets = profile[name]

            if ftype == "sum":
                buckets[bucket_key] = buckets.get(bucket_key, 0) + event[feature["field"]]
            elif ftype == "count":
                buckets[bucket_key] = buckets.get(bucket_key, 0) + 1
            elif ftype == "unique":
                if bucket_key not in buckets:
                    buckets[bucket_key] = set()
                buckets[bucket_key].add(event[feature["field"]])