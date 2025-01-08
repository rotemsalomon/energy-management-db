import yaml

# Check if yaml is available
try:
    yaml_version = yaml.__version__
    print(f"yaml module is installed, version: {yaml_version}")
except ImportError:
    print("yaml module is not installed.")