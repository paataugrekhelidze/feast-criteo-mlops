
from feast import  FeatureService
from features import impression_transformed_view

user_activity = FeatureService(
    name="user_activity", features=[impression_transformed_view]
)