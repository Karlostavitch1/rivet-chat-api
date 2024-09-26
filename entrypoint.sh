#!/bin/sh

# Check if the config directory is empty
if [ -z "$(ls -A /usr/src/app/config)" ]; then
  echo "Copying config files..."
  cp -r /usr/src/app/config_backup/* /usr/src/app/config/
fi

# Check if the rivet directory is empty
if [ -z "$(ls -A /usr/src/app/rivet)" ]; then
  echo "Copying rivet files..."
  cp -r /usr/src/app/rivet_backup/* /usr/src/app/rivet/
fi

# Start the application
exec "$@"