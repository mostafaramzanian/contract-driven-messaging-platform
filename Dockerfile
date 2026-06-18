# Official Node base image
FROM node:22-alpine

# Working directory inside the container
WORKDIR /usr/src/app

# Create the logs directory
RUN mkdir -p logs

# Copy config files and install dependencies first for better layer caching
COPY package*.json ./

# If you have network issues installing packages, try --network=host
RUN npm install

# Copy the rest of the project
COPY . .

# Build both NestJS applications (gateway + messaging)
RUN npm run build
RUN npx nest build messaging

# Run in production mode
CMD ["npm", "run", "start:prod"]
