from dataclasses import dataclass, fields
from datetime import datetime

@dataclass(init=False)
class BackwardCompatibleSpec:
    """
    Represents a backward compatible specification.
    """
    created_at: datetime
    updated_at: datetime
    created_by: str
    updated_by: str

    def __init__(self, **kwargs):
        print("Initializing BackwardCompatibleSpec instance...")
        # Initialize attributes based on provided keyword arguments
        for field in fields(self):
            if field.name in kwargs:
                setattr(self, field.name, kwargs[field.name])
                # print(f"Setting attribute {field.name} with value {kwargs[field.name]}")

# Instantiate a subclass (FlowSpec) with keyword arguments
class FlowSpec(BackwardCompatibleSpec):
    pass

# Example usage
kwargs = {
    'created_at': datetime.now(),
    'updated_at': datetime.now(),
    'created_by': 'user1',
    'updated_by': 'user2',
    'other_attribute': 'value'  # Additional attribute not defined in BackwardCompatibleSpec
}

# Create an instance of FlowSpec
flow_spec_instance = FlowSpec(**kwargs)

# Print the instance directly
print(flow_spec_instance)
