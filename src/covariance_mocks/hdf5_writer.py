"""
HDF5 Writer Module

Handles parallel and single-process HDF5 file writing for galaxy catalogs.
This module contains the complex collective I/O operations that will be reused
across 40,000+ mock generation runs.

Parallel HDF5 writing with MPI collective I/O operations.
"""

import os
import numpy as np
import h5py


def write_single_hdf5(galcat, plot_logmhost, plot_halo_radius, plot_halo_pos, plot_halo_vel, output_path, Lbox):
    """
    Write galaxy catalog to HDF5 file for single process.
    
    Parameters
    ----------
    galcat : dict
        Galaxy catalog from rgrspit_diffsky containing galaxy properties
    plot_logmhost : array_like
        Log10 halo masses used for galaxy generation  
    plot_halo_radius : array_like
        Halo virial radii in Mpc/h
    plot_halo_pos : array_like, shape (N_halos, 3)
        Halo positions in Mpc/h
    plot_halo_vel : array_like, shape (N_halos, 3)
        Halo velocities in km/s
    output_path : str
        Full path for output HDF5 file
    Lbox : float
        Simulation box size in Mpc/h
        
    Notes
    -----
    - Creates directory structure if it doesn't exist
    - Saves galaxy properties under 'galaxies/' group
    - Saves halo properties under 'halos/' group  
    - Includes metadata attributes: Lbox, z_obs, lgmp_min, n_halos, n_galaxies
    - Handles structured data by saving components separately
    """
    from . import CURRENT_Z_OBS, LGMP_MIN, SIMULATION_BOX, CURRENT_PHASE, CURRENT_REDSHIFT
    
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
    """
    Write galaxy catalog using parallel HDF5 for multiple MPI ranks.
    
    Coordinates collective I/O operations across MPI ranks to write a single
    HDF5 file containing galaxies and halos from all processes.
    
    Parameters
    ----------
    galcat : dict
        Galaxy catalog from rgrspit_diffsky for this rank
    plot_logmhost : array_like
        Log10 halo masses for this rank
    plot_halo_radius : array_like  
        Halo virial radii for this rank in Mpc/h
    plot_halo_pos : array_like, shape (N_halos, 3)
        Halo positions for this rank in Mpc/h
    plot_halo_vel : array_like, shape (N_halos, 3)
        Halo velocities for this rank in km/s
    output_path : str
        Full path for output HDF5 file
    rank : int
        MPI rank of this process
    size : int
        Total number of MPI processes
    comm : MPI.Comm
        MPI communicator for collective operations
    Lbox : float
        Simulation box size in Mpc/h
        
    Notes
    -----
    - Uses MPI collective operations to coordinate writes
    - Gathers counts and calculates offsets for contiguous data layout
    - All ranks write to same file using parallel HDF5
    - Rank 0 writes metadata and creates file structure
    - Includes temporary rank files cleanup after successful write
    - Handles galaxy and halo data with proper offset calculations
    """
    from . import CURRENT_Z_OBS, LGMP_MIN, SIMULATION_BOX, CURRENT_PHASE, CURRENT_REDSHIFT
    
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
    """Combine MPI rank files into final HDF5 catalog (legacy function - not used in parallel HDF5)"""
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
            print(f"Cleaned temporary file: {rank_path}")
