from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.types import Int64, String
from data_sources import criteo_data_source
from entities import user

impression_transformed_view = FeatureView(
    name="impression_transformed_view",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="campaign", dtype=Int64),
        Field(name="cat1", dtype=Int64),
        Field(name="cat2", dtype=Int64),
        Field(name="cat3", dtype=Int64),
        Field(name="cat4", dtype=Int64),
        Field(name="cat5", dtype=Int64),
        Field(name="cat6", dtype=Int64),
        Field(name="cat7", dtype=Int64),
        Field(name="cat8", dtype=Int64),
        Field(name="cat9", dtype=Int64),
        Field(name="last_5_clicks", dtype=String),
        Field(name="last_5_conversions", dtype=String),
    ],
    online=True,
    source=criteo_data_source
)