# Docker compose quick guide

When running the app as a service, it is useful to use docker-compose for easier configuraiton.

Create `docker-compose.yml` file somewhere on your machine.

Adjust following example (see [.env.sample](../.env.sample) for configuration options).

```yaml
version: '2'
services:
  nanit:
    # Image to pull, adjust the :suffix for your version tag
    image: indiefan/nanit:latest
    # Makes the container auto-start whenever you restart your computer
    restart: unless-stopped
    # Expose the RTMP port
    ports:
    - 1935:1935
    # Configuration (see .env.sample file for all the options)
    # Notice: Mind the quotes, whole pairs are quoted instead of just values. If your password contains $ character, replace it with double $$ to avoid interpolation.
    environment:
    - "NANIT_EMAIL=your@email.tld"
    - "NANIT_PASSWORD=XXXXXXXXXXXXX"
    - "NANIT_RTMP_ADDR=xxx.xxx.xxx.xxx:1935"
```

## Control the app container

Run in the same directory as your `docker-compose.yml` file

```bash
# Start the app
docker-compose up -d

# See the logs (Use Ctrl+C to terminate)
docker-compose logs -f nanit

# Stop the app
docker-compose stop

# Upgrade the app (ie. after you have changed the version tag or to pull fresh dev image)
docker-compose pull  # pulls fresh image
docker-compose down  # removes previously created container
docker-compose up -d # creates new container with fresh image (do this after every change in the docker-compose file)

# Uninstall the app
docker-compose down
```

## Run from the source of this repository

[deploy/docker-compose.yml](../deploy/docker-compose.yml) builds the image on the machine that runs it, from the repository as it is checked out there. No image registry and no sign-in to one is needed.

```bash
git clone https://github.com/Mr-Tibz/home_assistant_nanit.git
cd home_assistant_nanit
```

Put the two settings that are needed in the file `deploy/.env`. Compose reads that file at every run, and git leaves it alone:

```bash
# The address of this machine that the camera can reach, with the port
NANIT_RTMP_ADDR=192.168.1.10:1935
# The folder on this machine for session.json
NANIT_DATA_PATH=/srv/nanit-data
```

Make `session.json`, which is the sign-in to Nanit, with the setup script of the image. It asks for the e-mail address, the password and the code that Nanit sends:

```bash
docker compose -f deploy/docker-compose.yml run --rm --no-deps --entrypoint /app/scripts/init-nanit.sh nanit
```

Start the program:

```bash
docker compose -f deploy/docker-compose.yml up -d
```

Keep the folder behind `NANIT_DATA_PATH`, and an update needs no new sign-in. To update, fetch the new source and deploy again. The image is built again each time:

```bash
git pull
docker compose -f deploy/docker-compose.yml up -d
```

The base images (Go and Debian) are fetched once and then kept by Docker. To get their newer versions too, run `docker compose -f deploy/docker-compose.yml build --pull` before the `up`.

The top of the compose file lists the other settings, for example how to put the container on a Docker network that exists already, so that a media server on that network can read the stream.

### Automatic updates with Portainer

A Portainer stack can follow this repository and deploy again when it changes:

1. Stacks, Add stack, Repository.
2. Repository URL: the address of this repository. Compose path: `deploy/docker-compose.yml`.
3. Switch on GitOps updates, with polling.
4. Add the environment variables `NANIT_RTMP_ADDR` and `NANIT_DATA_PATH`.

Each update stops the container for the time of a restart. The program asks the camera to stop its stream first, so the new container gets the stream at once.
