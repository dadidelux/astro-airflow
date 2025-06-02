# How to Install and Start Airflow Using Astro

## Prerequisites
- Docker installed on your system.
- Astro CLI installed. You can download it from [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli).

## Steps

1. **Install Astro CLI**
   - Follow the instructions on the [Astro CLI installation guide](https://docs.astronomer.io/astro/cli/install-cli).

2. **Create a New Astro Project**
   ```bash
   astro dev init
   ```
   This will create a new Astro project in your current directory.

3. **Start Airflow Locally**
   ```bash
   astro dev start
   ```
   This command will start Airflow locally using Docker.

4. **Access the Airflow Webserver**
   - Open your browser and navigate to `http://localhost:8080`.
   - Use the default credentials:
     - Username: `admin`
     - Password: `admin`

5. **Stop Airflow**
   ```bash
   astro dev stop
   ```
   This will stop the Airflow containers.

## Additional Resources
- [Astro Documentation](https://docs.astronomer.io/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)