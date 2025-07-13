"""Core logic for SLURM-based mock generation testing.

This module provides shared functions for both pytest and shell script workflows,
ensuring consistent behavior across testing approaches.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class MockGenerationConfig:
    """Configuration for mock generation runs."""
    
    def __init__(
        self,
        output_dir: str,
        force_run: bool = False,
        test_mode: bool = False,
        n_gen: int = 5000,
        time_limit: int = 10,
        nersc_account: str = "m4943"
    ):
        self.output_dir = Path(output_dir)
        self.force_run = force_run
        self.test_mode = test_mode
        self.n_gen = n_gen
        self.time_limit = time_limit
        self.nersc_account = nersc_account
        
        # Ensure output directory exists (handle invalid paths gracefully)
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except (PermissionError, OSError) as e:
            # Let the calling code handle invalid paths
            pass
        
        # Set output filenames based on test mode
        if self.test_mode:
            self.output_catalog = f"mock_AbacusSummit_small_c000_ph3000_z1.100_test{self.n_gen}.hdf5"
            self.output_plot = f"halo_galaxy_scatter_AbacusSummit_small_c000_ph3000_z1.100_test{self.n_gen}.png"
        else:
            self.output_catalog = "mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5"
            self.output_plot = "halo_galaxy_scatter_AbacusSummit_small_c000_ph3000_z1.100.png"
    
    @property
    def catalog_path(self) -> Path:
        """Full path to output catalog file."""
        return self.output_dir / self.output_catalog
    
    @property
    def plot_path(self) -> Path:
        """Full path to output plot file."""
        return self.output_dir / self.output_plot


def get_script_dir() -> Path:
    """Get the scripts directory relative to this file."""
    return Path(__file__).parent.parent / "scripts"


def load_environment() -> Dict[str, str]:
    """Load the conda environment and return environment variables."""
    script_dir = get_script_dir()
    load_env_script = script_dir / "load_env.sh"
    
    # Source the environment script and capture environment
    cmd = f"source {load_env_script} && env"
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, executable="/bin/bash"
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Failed to load environment: {result.stderr}")
    
    # Parse environment variables
    env_vars = {}
    for line in result.stdout.strip().split('\n'):
        if '=' in line:
            key, value = line.split('=', 1)
            env_vars[key] = value
    
    return env_vars


def build_srun_command(config: MockGenerationConfig) -> List[str]:
    """Build the srun command for galaxy generation."""
    script_dir = get_script_dir()
    generate_script = script_dir / "generate_single_mock.py"
    
    base_cmd = ["srun"]
    
    if config.test_mode:
        # Test mode: single GPU for quick testing
        base_cmd.extend([
            "-n", "1",
            "-c", "8", 
            "--qos=interactive",
            "-N", "1",
            "--time=15",
            "-C", "gpu",
            "-A", config.nersc_account,
            "--gpus-per-node=1"
        ])
    else:
        # Production mode: multiple GPUs with MPI
        base_cmd.extend([
            "-n", "6",
            "--gpus-per-node=3",
            "-c", "32",
            "--qos=interactive", 
            "-N", "2",
            f"--time={config.time_limit}",
            "-C", "gpu",
            "-A", config.nersc_account
        ])
    
    # Add the Python script and arguments
    base_cmd.extend([
        "python", str(generate_script),
        "nersc", str(config.output_dir)
    ])
    
    if config.test_mode:
        base_cmd.extend(["--test", str(config.n_gen)])
    
    return base_cmd


def run_galaxy_generation(config: MockGenerationConfig) -> Tuple[bool, str]:
    """Execute galaxy generation with SLURM.
    
    Returns:
        Tuple of (success, output_message)
    """
    # Check if output already exists and force flag is not set
    if config.catalog_path.exists() and not config.force_run:
        return True, f"Galaxy catalog already exists: {config.catalog_path}"
    
    # Load environment
    env_vars = load_environment()
    
    # Build and execute srun command
    cmd = build_srun_command(config)
    
    # Execute with proper environment and output handling
    if config.test_mode:
        # For test mode, use unbuffered output
        result = subprocess.run(cmd, env=env_vars, capture_output=True, text=True)
    else:
        # For production mode, use stdbuf for unbuffered output
        stdbuf_cmd = ["stdbuf", "-o0", "-e0"] + cmd
        result = subprocess.run(stdbuf_cmd, env=env_vars, capture_output=True, text=True)
    
    # Check if generation was successful
    if result.returncode != 0:
        return False, f"Galaxy generation failed with return code {result.returncode}: {result.stderr}"
    
    if not config.catalog_path.exists():
        return False, f"Galaxy catalog generation failed - missing file: {config.catalog_path}"
    
    return True, f"Galaxy generation completed successfully: {config.catalog_path}"


def run_plotting(config: MockGenerationConfig) -> Tuple[bool, str]:
    """Execute plotting script for generated catalog.
    
    Returns:
        Tuple of (success, output_message)
    """
    script_dir = get_script_dir()
    plot_script = script_dir / "plot_mock_catalog.py"
    
    # Load environment
    env_vars = load_environment()
    
    # Build plotting command
    cmd = ["python", str(plot_script), str(config.catalog_path)]
    if config.test_mode:
        cmd.extend(["--max-halos", str(config.n_gen)])
    
    # Execute plotting
    result = subprocess.run(cmd, env=env_vars, capture_output=True, text=True)
    
    if result.returncode != 0:
        return False, f"Plotting failed with return code {result.returncode}: {result.stderr}"
    
    return True, f"Plotting completed successfully: {config.plot_path}"


def run_full_pipeline(config: MockGenerationConfig) -> Tuple[bool, List[str]]:
    """Execute the complete mock generation pipeline.
    
    Returns:
        Tuple of (overall_success, list_of_messages)
    """
    messages = []
    
    # Run galaxy generation
    gen_success, gen_message = run_galaxy_generation(config)
    messages.append(gen_message)
    
    if not gen_success:
        return False, messages
    
    # Run plotting
    plot_success, plot_message = run_plotting(config)
    messages.append(plot_message)
    
    return plot_success, messages


def parse_shell_args(args: List[str]) -> Tuple[bool, bool, bool]:
    """Parse shell script style arguments.
    
    Returns:
        Tuple of (force_run, test_mode, verbose)
    """
    force_run = False
    test_mode = False
    verbose = False
    
    for arg in args:
        if arg == "--force":
            force_run = True
        elif arg == "--test":
            test_mode = True
        elif arg == "--verbose" or arg == "-v":
            verbose = True
        else:
            raise ValueError(f"Unknown argument: {arg}")
    
    return force_run, test_mode, verbose


def main():
    """Command-line interface for shared core logic."""
    if len(sys.argv) < 2:
        print("Usage: python integration_core.py <output_dir> [--force] [--test] [--verbose|-v]")
        sys.exit(1)
    
    output_dir = sys.argv[1]
    args = sys.argv[2:]
    
    try:
        force_run, test_mode, verbose = parse_shell_args(args)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Get output directory from environment or use provided
    if not output_dir and os.getenv("GRS_COV_MOCKS_DIR"):
        output_dir = os.getenv("GRS_COV_MOCKS_DIR")
    elif not output_dir:
        output_dir = f"{os.getenv('SCRATCH', '/tmp')}/grspit/covariance_mocks/data"
    
    if verbose:
        print(f"Mock generation configuration:")
        print(f"  Output directory: {output_dir}")
        print(f"  Test mode: {test_mode}")
        print(f"  Force run: {force_run}")
        print(f"  Time limit: {10 if not test_mode else 5} minutes")
        print()
    
    config = MockGenerationConfig(
        output_dir=output_dir,
        force_run=force_run,
        test_mode=test_mode
    )
    
    if verbose:
        print(f"Expected output files:")
        print(f"  Catalog: {config.catalog_path}")
        print(f"  Plot: {config.plot_path}")
        print()
        print("Starting pipeline execution...")
        print()
    
    success, messages = run_full_pipeline(config)
    
    for message in messages:
        print(message)
    
    if verbose:
        if success:
            print()
            print("Pipeline completed successfully!")
            if config.catalog_path.exists():
                size_mb = config.catalog_path.stat().st_size / (1024 * 1024)
                print(f"  Catalog size: {size_mb:.1f} MB")
            plot_files = list(config.output_dir.glob("*.png"))
            if plot_files:
                print(f"  Plot files: {len(plot_files)} created")
        else:
            print()
            print("Pipeline failed!")
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()