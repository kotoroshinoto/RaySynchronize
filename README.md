# RaySynchronize

Trying out the idea of creating representations of synchronization objects for use in Ray.

## Directory Structure

- **src/raysynchronize/**
  - **ray_utils/**: Utility functions for Ray synchronization.
  - **ray_actors/**: Actors for Ray synchronization.
  - **local_interface/**: Local interface for Ray synchronization.
  - **raysynchronize.py**: Main module for RaySynchronize.

- **tests/**: Test cases for RaySynchronize.

## Usage

To use the RaySynchronize project, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/RaySynchronize.git
   ```
2. Install Dependencies:
   - All dependencies 
      ```bash
      pdm install -G :all
      ```
   - Standard dependencies (for use, but not testing or development)
     ```bash
     pdm install
     ```
   - Development dependencies
     ```bash
     pdm install -G dev
     ```
3. Run tests
   ```bash
   nox -s test
   ```
