FROM node:9.4.0-alpine

LABEL maintainer="Randy Du (randydu@gmail.com)"

COPY . /home/node

USER node
WORKDIR /home/node

CMD ["node", "index.js"]