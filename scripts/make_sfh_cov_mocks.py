"""Generate covariance mocks for AbacusSummit halo catalogs"""

import sys
import argparse
import os
import numpy as np

# MPI setup
try:
    from mpi4py import MPI
    MPI_AVAILABLE = True
except ImportError:
    MPI_AVAILABLE = False

# JAX-dependent imports done after MPI setup

# AbacusSummit data configuration
ABACUS_BASE_PATH = "/global/cfs/cdirs/desi/public/cosmosim/AbacusSummit"
SIMULATION_SUITE = "small"
SIMULATION_BOX = "AbacusSummit_small_c000"

# Configuration for current run
CURRENT_PHASE = "ph3000"
CURRENT_REDSHIFT = "z1.100"
CURRENT_Z_OBS = 1.1
LGMP_MIN = 10.0  # log10 minimum halo mass

def build_abacus_path(base_path, suite, box, phase, redshift):
    """Build full path to AbacusSummit halo catalog directory"""
    return os.path.join(base_path, suite, f"{box}_{phase}", "halos", redshift)


def generate_mock_for_catalog(catalog_path, output_path, n_gen=None):
    """Generate mock galaxy catalog for a single AbacusSummit halo catalog using MPI if available"""
    
    # Initialize MPI if available
    if MPI_AVAILABLE:
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()
        
        if rank == 0:
            print(f"Starting MPI job with {size} processes")

    else:
        rank = 0
        size = 1
        comm = None
        print("Running in single-process mode")

    if MPI_AVAILABLE and size > 1:
        import os
        # Configure JAX for distributed use
        os.environ['JAX_PLATFORMS'] = ''
        os.environ['JAX_DISTRIBUTED_INITIALIZE'] = 'false'
    
    # Import JAX after MPI setup
    import jax
    from jax import random as jran
    
    if MPI_AVAILABLE and size > 1:
        jax.distributed.initialize()
    
    # Report JAX configuration
    print(f"Rank {rank}: JAX backend: {jax.default_backend()}")
    print(f"Rank {rank}: JAX devices: {jax.devices()}")

    # Import JAX-dependent packages
    from dsps.cosmology import DEFAULT_COSMOLOGY
    from rgrspit_diffsky import mc_galpop
    from rgrspit_diffsky.data_loaders import load_abacus

    # Generate random key
    ran_key = jran.key(0)
    
    # Load halo catalog (all ranks load the same data initially)
    halo_catalog = load_abacus.load_abacus_halo_catalog(catalog_path)

    # Extract required variables from catalog
    mass = halo_catalog['mass']
    
    # Filter out zero masses and apply minimum mass cut
    min_mass = 10**LGMP_MIN
    valid_mask = (mass > 0) & (mass >= min_mass)
    
    if np.sum(valid_mask) == 0:
        raise ValueError(f"No halos above minimum mass {min_mass:.2e}")
    
    logmhost = np.log10(mass[valid_mask])
    halo_radius = halo_catalog['radius'][valid_mask]
    halo_pos = halo_catalog['pos'][valid_mask]
    halo_vel = halo_catalog['vel'][valid_mask]
    Lbox = halo_catalog['lbox']
    
    # Convert halo positions from [-Lbox/2, Lbox/2] to [0, Lbox]
    halo_pos = halo_pos + Lbox/2
    
    if rank == 0:
        print(f"Loaded {len(logmhost)} halos above mass threshold from {len(mass)} total halos")
    
    # Apply test mode limitation BEFORE slab decomposition (original logic)
    # Select the n_gen halos with smallest x-coordinates from the full catalog
    if n_gen is not None:
        if rank == 0:
            print(f"Test mode: selecting {n_gen} halos with smallest x-coordinates")
        
        # Sort by x-coordinate and take the first n_gen halos
        x_coords = halo_pos[:, 0]
        sorted_indices = np.argsort(x_coords)
        test_indices = sorted_indices[:n_gen]
        
        # Apply test mode limitation to all arrays
        logmhost = logmhost[test_indices]
        halo_radius = halo_radius[test_indices]
        halo_pos = halo_pos[test_indices]
        halo_vel = halo_vel[test_indices]
        
        if rank == 0:
            print(f"Test mode: reduced to {len(logmhost)} halos, x-range [{halo_pos[:, 0].min():.1f}, {halo_pos[:, 0].max():.1f}]")
    
    # Implement slab decomposition based on x-coordinate
    # Each MPI rank gets a slab: [rank * Lbox/size, (rank+1) * Lbox/size)
    y_min = rank * Lbox / size
    y_max = (rank + 1) * Lbox / size
    
    # Select halos in this rank's slab
    slab_mask = (halo_pos[:, 1] >= y_min) & (halo_pos[:, 1] < y_max)
    
    # Handle the last rank to include the boundary
    if rank == size - 1:
        slab_mask = (halo_pos[:, 1] >= y_min) & (halo_pos[:, 1] <= y_max)

    # Extract halos for this rank
    rank_logmhost = logmhost[slab_mask]
    rank_halo_radius = halo_radius[slab_mask]
    rank_halo_pos = halo_pos[slab_mask]
    rank_halo_vel = halo_vel[slab_mask]

    print(f"Rank {rank}: processing {len(rank_logmhost)} halos in y-slab [{y_min:.1f}, {y_max:.1f}]")

    # Use rank-specific halos for galaxy generation
    plot_logmhost = rank_logmhost
    plot_halo_radius = rank_halo_radius
    plot_halo_pos = rank_halo_pos
    plot_halo_vel = rank_halo_vel
    
    print(f"Rank {rank}: using {len(plot_logmhost)} halos for galaxy generation")
    
    # Convert to JAX arrays with proper dtypes
    import jax.numpy as jnp
    plot_logmhost = jnp.asarray(plot_logmhost, dtype=jnp.float32)
    plot_halo_radius = jnp.asarray(plot_halo_radius, dtype=jnp.float32)
    plot_halo_pos = jnp.asarray(plot_halo_pos, dtype=jnp.float32)
    plot_halo_vel = jnp.asarray(plot_halo_vel, dtype=jnp.float32)
    Lbox = float(Lbox)
    
    # Generate mock galaxy catalog for selected halos
    galcat = mc_galpop.mc_galpop_synthetic_subs(
        ran_key,
        plot_logmhost,
        plot_halo_radius,
        plot_halo_pos,
        plot_halo_vel,
        CURRENT_Z_OBS,
        LGMP_MIN,
        DEFAULT_COSMOLOGY,
        Lbox,
    )
    
    print(f"Rank {rank}: generated mock with {len(galcat['pos'])} galaxies from {len(plot_logmhost)} halos")
    # Use parallel HDF5 writing instead of concatenation approach
    if MPI_AVAILABLE and comm is not None and size > 1:
        # Parallel HDF5 writing for multiple ranks
        write_parallel_hdf5(galcat, plot_logmhost, plot_halo_radius, plot_halo_pos, plot_halo_vel, 
                           output_path, rank, size, comm, Lbox)  
    else:
        # Single process - write directly to output file
        write_single_hdf5(galcat, plot_logmhost, plot_halo_radius, plot_halo_pos, plot_halo_vel, 
                         output_path, Lbox)
    
    print(f"Galaxy catalog saved to: {output_path}")
    
    # Explicit MPI cleanup to ensure clean exit
    if MPI_AVAILABLE and comm is not None and size > 1:
        print(f"Rank {rank}: Starting MPI finalization")
        comm.Barrier()  # Ensure all ranks finish
        print(f"Rank {rank}: MPI finalization complete")
    
    return galcat


def write_single_hdf5(galcat, plot_logmhost, plot_halo_radius, plot_halo_pos, plot_halo_vel, output_path, Lbox):
    import h5py
    """Write galaxy catalog to HDF5 file for single process."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with h5py.File(output_path, 'w') as f:
        # Save galaxy properties
        for key, value in galcat.items():
            try:
                # Convert to numpy array with consistent shape
                arr_value = np.array(value)
                f.create_dataset(f'galaxies/{key}', data=arr_value)
            except ValueError as e:
                # Handle structured data or complex objects
                print(f"Skipping {key} due to shape issue: {e}")
                # For structured data like DiffmahParams, save components separately
                if hasattr(value, '_fields'):
                    for field_name in value._fields:
                        field_value = getattr(value, field_name)
                        f.create_dataset(f'galaxies/{key}_{field_name}', data=np.array(field_value))
        
        # Save halo properties used for generation
        f.create_dataset('halos/logmhost', data=np.array(plot_logmhost))
        f.create_dataset('halos/radius', data=np.array(plot_halo_radius))
        f.create_dataset('halos/pos', data=np.array(plot_halo_pos))
        f.create_dataset('halos/vel', data=np.array(plot_halo_vel))
        
        # Save metadata
        f.attrs['Lbox'] = Lbox
        f.attrs['z_obs'] = CURRENT_Z_OBS
        f.attrs['lgmp_min'] = LGMP_MIN
        f.attrs['n_halos'] = len(plot_logmhost)
        f.attrs['n_galaxies'] = len(galcat['pos'])
        f.attrs['simulation_box'] = SIMULATION_BOX
        f.attrs['phase'] = CURRENT_PHASE
        f.attrs['redshift'] = CURRENT_REDSHIFT
        f.attrs['mpi_rank'] = 0
        f.attrs['mpi_size'] = 1


def write_parallel_hdf5(galcat, plot_logmhost, plot_halo_radius, plot_halo_pos, plot_halo_vel, 
                       output_path, rank, size, comm, Lbox):
    import h5py
    """Write galaxy catalog using parallel HDF5 for multiple MPI ranks."""
    

    # Step 1: Gather galaxy and halo counts from all ranks
    local_n_galaxies = len(galcat['pos'])
    local_n_halos = len(plot_logmhost)
    
    # Gather all counts on rank 0
    all_n_galaxies = comm.gather(local_n_galaxies, root=0)
    all_n_halos = comm.gather(local_n_halos, root=0)
    
    # Broadcast totals and offsets to all ranks
    if rank == 0:
        total_n_galaxies = sum(all_n_galaxies)
        total_n_halos = sum(all_n_halos)
        
        # Calculate galaxy offsets for each rank
        galaxy_offsets = [0]
        for i in range(size - 1):
            galaxy_offsets.append(galaxy_offsets[-1] + all_n_galaxies[i])
        
        # Calculate halo offsets for each rank  
        halo_offsets = [0]
        for i in range(size - 1):
            halo_offsets.append(halo_offsets[-1] + all_n_halos[i])
    else:
        total_n_galaxies = None
        total_n_halos = None
        galaxy_offsets = None
        halo_offsets = None
    
    # Broadcast to all ranks
    total_n_galaxies = comm.bcast(total_n_galaxies, root=0)
    total_n_halos = comm.bcast(total_n_halos, root=0)
    galaxy_offsets = comm.bcast(galaxy_offsets, root=0)
    halo_offsets = comm.bcast(halo_offsets, root=0)
    
    # Calculate this rank's slice indices
    galaxy_start = galaxy_offsets[rank]
    galaxy_end = galaxy_start + local_n_galaxies
    halo_start = halo_offsets[rank]
    halo_end = halo_start + local_n_halos
    
    print(f"Rank {rank}: writing galaxies [{galaxy_start}:{galaxy_end}] and halos [{halo_start}:{halo_end}]")
    comm.Barrier()
    
    # Step 2: Create file and datasets on all ranks simultaneously
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Gather dataset specifications from all ranks to ensure consistency
    
    # Define which fields are global/shared vs per-galaxy
    global_fields = {'t_table', 't0', 'z_obs', 't_obs'}
    
    local_dataset_specs = {}
    for key, value in galcat.items():
        try:
            arr_value = np.array(value)
            if len(arr_value.shape) <= 2:  # Only handle 1D and 2D arrays
                local_dataset_specs[key] = {
                    'shape': arr_value.shape,
                    'dtype': str(arr_value.dtype),
                    'is_global': key in global_fields
                }
        except ValueError:
            pass  # Skip problematic arrays
    
    # Gather all specs and take rank 0's specs as canonical
    all_specs = comm.gather(local_dataset_specs, root=0)
    if rank == 0:
        # Use rank 0's specs as the canonical set
        canonical_specs = all_specs[0]
    else:
        canonical_specs = None
    canonical_specs = comm.bcast(canonical_specs, root=0)
    
    with h5py.File(output_path, 'w', driver='mpio', comm=comm) as f:
        # All ranks create the same datasets based on rank 0's specs
        galaxy_datasets = {}
        for key, spec in canonical_specs.items():
            shape = spec['shape']
            dtype = spec['dtype']
            is_global = spec.get('is_global', False)
            
            if is_global:
                # Global/shared data: keep original shape
                if len(shape) == 0:  # Scalar
                    dset = f.create_dataset(f'galaxies/{key}', (), dtype=dtype)
                else:  # Shared array like t_table
                    dset = f.create_dataset(f'galaxies/{key}', shape, dtype=dtype)
            elif len(shape) == 1:
                # 1D array: (n_galaxies,)
                dset = f.create_dataset(f'galaxies/{key}', (total_n_galaxies,), dtype=dtype)
            elif len(shape) == 2:
                if key in ['log_mah_table', 'sfh_table', 'pos', 'vel']:
                    # Table arrays: (n_galaxies, n_features)
                    dset = f.create_dataset(f'galaxies/{key}', (total_n_galaxies, shape[1]), dtype=dtype)
                else:
                    # Parameter arrays: (n_params, n_galaxies)
                    dset = f.create_dataset(f'galaxies/{key}', (shape[0], total_n_galaxies), dtype=dtype)
            galaxy_datasets[key] = dset
        
        # Create halo datasets
        halo_datasets = {}
        halo_data = {
            'logmhost': plot_logmhost,
            'radius': plot_halo_radius,
            'pos': plot_halo_pos,
            'vel': plot_halo_vel
        }
        
        for key, value in halo_data.items():
            arr_value = np.array(value)
            if len(arr_value.shape) == 1:
                # 1D array: (n_halos,)
                dset = f.create_dataset(f'halos/{key}', (total_n_halos,), dtype=arr_value.dtype)
            elif len(arr_value.shape) == 2:
                if key in ['pos', 'vel']:
                    # Table arrays: (n_halos, n_features)
                    dset = f.create_dataset(f'halos/{key}', (total_n_halos, arr_value.shape[1]), dtype=arr_value.dtype)
                else:
                    # Parameter arrays: (n_params, n_halos)
                    dset = f.create_dataset(f'halos/{key}', (arr_value.shape[0], total_n_halos), dtype=arr_value.dtype)
            halo_datasets[key] = dset
        
        # Step 3: Each rank writes its data to its slice
        comm.Barrier()
        
        for key, dset in galaxy_datasets.items():
            if key not in galcat:
                continue
            arr_value = np.array(galcat[key])
            spec = canonical_specs[key]
            is_global = spec.get('is_global', False)
            
            if is_global:
                # Global data: only rank 0 writes, and writes entire array
                if rank == 0:
                    if len(arr_value.shape) == 0:  # Scalar
                        dset[()] = arr_value
                    else:  # Shared array like t_table
                        dset[:] = arr_value
            elif len(arr_value.shape) == 1:
                # 1D array: write to [start:end]
                if arr_value.shape[0] != (galaxy_end - galaxy_start):
                    continue
                dset[galaxy_start:galaxy_end] = arr_value
            elif len(arr_value.shape) == 2:
                if key in ['log_mah_table', 'sfh_table', 'pos', 'vel']:
                    # Table arrays: write to [start:end, :]
                    dset[galaxy_start:galaxy_end, :] = arr_value
                else:
                    # Parameter arrays: write to [:, start:end]
                    dset[:, galaxy_start:galaxy_end] = arr_value
        
        
        for key, dset in halo_datasets.items():
            arr_value = np.array(halo_data[key])
            if len(arr_value.shape) == 1:
                # 1D array: write to [start:end]
                dset[halo_start:halo_end] = arr_value
            elif len(arr_value.shape) == 2:
                if key in ['pos', 'vel']:
                    # Table arrays: write to [start:end, :]
                    dset[halo_start:halo_end, :] = arr_value
                else:
                    # Parameter arrays: write to [:, start:end]
                    dset[:, halo_start:halo_end] = arr_value
        
        
        # Step 4: ALL RANKS write metadata collectively
        comm.Barrier()
        
        # COLLECTIVE METADATA OPERATIONS - all ranks participate
        f.attrs['Lbox'] = Lbox
        f.attrs['z_obs'] = CURRENT_Z_OBS
        f.attrs['lgmp_min'] = LGMP_MIN
        f.attrs['n_halos'] = total_n_halos
        f.attrs['n_galaxies'] = total_n_galaxies
        f.attrs['simulation_box'] = SIMULATION_BOX
        f.attrs['phase'] = CURRENT_PHASE
        f.attrs['redshift'] = CURRENT_REDSHIFT
        f.attrs['mpi_parallel'] = True
        f.attrs['mpi_size'] = size

    comm.Barrier()


def combine_mpi_files(output_path, size):
    """Combine MPI rank files into final HDF5 catalog"""
    base_path, ext = os.path.splitext(output_path)
    
    # Read first file to get structure
    rank0_path = f"{base_path}_rank0000{ext}"
    
    combined_galaxies = {}
    combined_halos = {}
    total_n_halos = 0
    total_n_galaxies = 0
    
    # Read and combine all rank files
    for rank in range(size):
        rank_path = f"{base_path}_rank{rank:04d}{ext}"
        print(f"Reading rank file: {rank_path}")
        
        with h5py.File(rank_path, 'r') as f:
            # Combine galaxy data
            for key in f['galaxies'].keys():
                data = np.array(f[f'galaxies/{key}'])
                if key not in combined_galaxies:
                    combined_galaxies[key] = [data]
                else:
                    combined_galaxies[key].append(data)
            
            # Combine halo data
            for key in f['halos'].keys():
                data = np.array(f[f'halos/{key}'])
                if key not in combined_halos:
                    combined_halos[key] = [data]
                else:
                    combined_halos[key].append(data)
            
            # Sum counters
            total_n_halos += f.attrs['n_halos']
            total_n_galaxies += f.attrs['n_galaxies']
    
    # Write combined file
    with h5py.File(output_path, 'w') as f:
        # Save combined galaxy properties
        for key, data_list in combined_galaxies.items():
            
            # Different arrays have different conventions
            if len(data_list[0].shape) == 1:
                concat_axis = 0
            elif key in ['log_mah_table', 'sfh_table', 'pos', 'vel']:
                concat_axis = 0
            else:
                concat_axis = 1
            
            combined_data = np.concatenate(data_list, axis=concat_axis)
            f.create_dataset(f'galaxies/{key}', data=combined_data)
        
        # Save combined halo properties  
        for key, data_list in combined_halos.items():
            
            concat_axis = 1
            
            combined_data = np.concatenate(data_list, axis=concat_axis)
            f.create_dataset(f'halos/{key}', data=combined_data)
        
        # Save metadata from first file and update counters
        with h5py.File(rank0_path, 'r') as f0:
            for attr_name in f0.attrs.keys():
                if attr_name not in ['n_halos', 'n_galaxies', 'mpi_rank', 'mpi_size']:
                    f.attrs[attr_name] = f0.attrs[attr_name]
        
        f.attrs['n_halos'] = total_n_halos
        f.attrs['n_galaxies'] = total_n_galaxies
        f.attrs['mpi_combined'] = True
    
    # Clean up rank files
    for rank in range(size):
        rank_path = f"{base_path}_rank{rank:04d}{ext}"
        if os.path.exists(rank_path):
            os.remove(rank_path)
            print(f"Removed temporary file: {rank_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "machine", help="Machine name where script is run", choices=["nersc", "poboy"]
    )

    parser.add_argument("drnout", help="Output directory")
    
    parser.add_argument("--test", type=int, help="Test mode: use only N halos with smallest x coordinates")

    args = parser.parse_args()
    machine = args.machine
    drnout = args.drnout
    n_gen = args.test

    if machine == "nersc":
        # Build path for current configuration
        catalog_path = build_abacus_path(
            ABACUS_BASE_PATH, SIMULATION_SUITE, SIMULATION_BOX, 
            CURRENT_PHASE, CURRENT_REDSHIFT
        )
        
        # Verify path exists
        if not os.path.isdir(catalog_path):
            raise FileNotFoundError(f"AbacusSummit catalog not found: {catalog_path}")
        
        # Generate output filename
        if n_gen is not None:
            output_filename = f"mock_{SIMULATION_BOX}_{CURRENT_PHASE}_{CURRENT_REDSHIFT}_test{n_gen}.hdf5"
        else:
            output_filename = f"mock_{SIMULATION_BOX}_{CURRENT_PHASE}_{CURRENT_REDSHIFT}.hdf5"
        output_path = os.path.join(drnout, output_filename)
        
        # Generate galaxy catalog (JAX ops moved inside function)
        galcat = generate_mock_for_catalog(catalog_path, output_path, n_gen)
        print(f"Generated {len(galcat['pos'])} galaxies total")
        
    elif machine == "poboy":
        raise NotImplementedError("poboy machine not yet implemented")
