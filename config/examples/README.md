# Configuration Templates

This directory contains template configuration files for creating new productions.

## Usage

1. Copy a template file to `config/productions/`
2. Rename it to match your production name
3. Edit the configuration as needed for your specific production

## Templates

- **`covariance_template.yaml`** - Template for full-scale covariance productions
- **`test_template.yaml`** - Template for small-scale test productions

## Key Fields to Modify

- `production.name` - Must be unique for your production
- `production.description` - Describe your production purpose
- `science.redshifts` - Redshift values for your analysis
- `science.realizations` - Number and range of realizations
- `outputs.base_path` - Where to store your output files

## See Also

- `config/defaults/` - Machine-specific default configurations
- `config/schemas/` - Configuration validation schemas
- `config/productions/` - Actual production configurations