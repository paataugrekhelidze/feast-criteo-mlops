
from feast import RedshiftSource

# Option 1: Point to the output of the Ray ETL job
# transformed_source = FileSource(
#     name="criteo_transformed",
#     path="s3://<MY-BUCKET>/feast/criteo/transformed/features/",
#     timestamp_field="event_timestamp",
#     file_format=ParquetFormat(),
# )

# Option 2: Point to the external Redshift spectrum table. Not Ideal, does not allow neted statemsnt in get_historical_features().
# criteo_data_source = RedshiftSource(
#     name="criteo_processed",
#     query="SELECT * FROM criteo.features",
#     timestamp_field="event_timestamp"
# )

# Option 3: Point to a local redshift table. 
criteo_data_source = RedshiftSource(
    name="criteo_processed",
    schema="public",
    table="temp_feast_entities",
    # The event timestamp is used for point-in-time joins and for ensuring only
    # features within the TTL are returned
    timestamp_field="event_timestamp"
)