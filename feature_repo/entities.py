from feast import Entity

user = Entity(name="user", join_keys=["uid"])
