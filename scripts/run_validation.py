#!/usr/bin/env python3
"""Validation script to compare generated catalogs against reference.

This script provides a command-line interface for catalog validation
that can be used independently of pytest.
"""

import sys
import argparse
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tests.test_catalog_validation import (
    load_hdf5_datasets,
    compare_datasets,
    REFERENCE_CATALOG
)
from tests.integration_core import MockGenerationConfig, run_full_pipeline


def validate_catalog(catalog_path, reference_path=None, tolerance=1e-10):
    """Validate a catalog against reference."""
    if reference_path is None:
        reference_path = REFERENCE_CATALOG
    
    reference_path = Path(reference_path)
    catalog_path = Path(catalog_path)
    
    print(f"Validating catalog: {catalog_path}")
    print(f"Against reference: {reference_path}")
    
    if not reference_path.exists():
        print(f"ERROR: Reference catalog not found: {reference_path}")
        return False
    
    if not catalog_path.exists():
        print(f"ERROR: Test catalog not found: {catalog_path}")
        return False
    
    try:
        print("Loading reference catalog...")
        ref_data = load_hdf5_datasets(reference_path)
        print(f"  Found {len(ref_data)} datasets in reference")
        
        print("Loading test catalog...")
        test_data = load_hdf5_datasets(catalog_path)
        print(f"  Found {len(test_data)} datasets in test catalog")
        
        print(f"Comparing with tolerance={tolerance}...")
        differences = compare_datasets(ref_data, test_data, tolerance=tolerance)
        
        if len(differences) == 0:
            print("✅ VALIDATION PASSED: Catalogs are identical")
            return True
        else:
            print("❌ VALIDATION FAILED: Differences found:")
            for diff in differences:
                print(f"  - {diff}")
            return False
            
    except Exception as e:
        print(f"ERROR during validation: {e}")
        return False


def generate_and_validate(output_dir, test_mode=False, tolerance=1e-10):
    """Generate a new catalog and validate it against reference."""
    print(f"Generating catalog in: {output_dir}")
    print(f"Test mode: {test_mode}")
    
    config = MockGenerationConfig(
        output_dir=output_dir,
        test_mode=test_mode,
        force_run=True
    )
    
    print("Running pipeline...")
    success, messages = run_full_pipeline(config)
    
    for message in messages:
        print(f"  {message}")
    
    if not success:
        print("❌ Pipeline failed")
        return False
    
    print("Pipeline completed successfully")
    print(f"Generated catalog: {config.catalog_path}")
    
    # Validate against reference
    return validate_catalog(config.catalog_path, tolerance=tolerance)


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(description="Validate galaxy catalogs against reference")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Validate existing catalog
    validate_parser = subparsers.add_parser("validate", help="Validate existing catalog")
    validate_parser.add_argument("catalog", help="Path to catalog to validate")
    validate_parser.add_argument("--reference", help="Path to reference catalog", 
                                default=str(REFERENCE_CATALOG))
    validate_parser.add_argument("--tolerance", type=float, default=1e-10,
                                help="Tolerance for floating point comparisons")
    
    # Generate and validate
    generate_parser = subparsers.add_parser("generate", help="Generate new catalog and validate")
    generate_parser.add_argument("output_dir", help="Output directory for generation")
    generate_parser.add_argument("--test-mode", action="store_true", 
                                help="Use test mode (smaller catalog)")
    generate_parser.add_argument("--tolerance", type=float, default=1e-10,
                                help="Tolerance for floating point comparisons")
    
    # Show reference info
    info_parser = subparsers.add_parser("info", help="Show reference catalog information")
    
    args = parser.parse_args()
    
    if args.command == "validate":
        success = validate_catalog(args.catalog, args.reference, args.tolerance)
        sys.exit(0 if success else 1)
        
    elif args.command == "generate":
        success = generate_and_validate(args.output_dir, args.test_mode, args.tolerance)
        sys.exit(0 if success else 1)
        
    elif args.command == "info":
        if not REFERENCE_CATALOG.exists():
            print(f"Reference catalog not found: {REFERENCE_CATALOG}")
            sys.exit(1)
        
        try:
            ref_data = load_hdf5_datasets(REFERENCE_CATALOG)
            print(f"Reference catalog: {REFERENCE_CATALOG}")
            print(f"File size: {REFERENCE_CATALOG.stat().st_size:,} bytes")
            print(f"Datasets ({len(ref_data)}):")
            
            for name, data in ref_data.items():
                print(f"  {name}: shape={data.shape}, dtype={data.dtype}")
                if data.size > 0 and data.dtype.kind in 'fc':  # float or complex
                    print(f"    range: [{data.min():.6g}, {data.max():.6g}]")
                    
        except Exception as e:
            print(f"Error reading reference catalog: {e}")
            sys.exit(1)
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()