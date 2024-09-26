# Use the official Node.js image as the base image
FROM node:18

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application code
COPY . .

# Copy the config and rivet directories to backup locations
RUN cp -r ./config ./config_backup
RUN cp -r ./rivet ./rivet_backup

# Copy the entrypoint script
COPY entrypoint.sh /usr/src/app/entrypoint.sh
RUN chmod +x /usr/src/app/entrypoint.sh

# Build the TypeScript code
RUN npm run build

# Expose the port the app runs on
EXPOSE 3100

# Command to run the application
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["npm", "start"]