def deep_merge(source: dict, destination: dict) -> dict:
    """
    Recursively merges source dict into destination dict.
    """
    for key, value in source.items():
        if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
            destination[key] = deep_merge(value, destination[key])
        else:
            destination[key] = value
    return destination
