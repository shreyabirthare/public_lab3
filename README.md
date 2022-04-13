Compsci 677: Distributed and Operating Systems
Spring 2022

# Lab 3: Caching, Replication and Fault Tolerance

## Team Members

You may work in groups of 2 for this lab assignment. Please list the names of the group members
here. You may replace this readme file with your own documentation, in which case, please list the
names of all team members at the top of the readme.

## Goals and Learning Outcomes

The lab has the following learning outcomes with regards to concepts covered in class.

TODO

## Lab Description

This project is based on lab 2. You can reuse some of the code you wrote in lab 2 if you want. You are going to implement a

1.  The toy store application consists of three microservices: a front-end service, a catalog
    service, and an order service.

2.  The front-end service exposes the following REST APIs as they were defined in lab2:

    *   `GET /products/<product_name>`
    *   `POST /orders`

    In addition, the front-end service will provide a new REST API that allows clients to query existing orders:

    *   `GET /orders/<order_number>`

        This API returns a JSON reply with a top-level `data` object with the three fields:
        `number`, `name`, and `quantity`. If the order number doesn't exist, a JSON reply with a
        top-level `error` object should be returned. The `error` object should contain two fields:
        `code` and `message`

    Since in this lab we will focus on higher level concepts, you CAN use a web framework like
    [`Django`](https://github.com/perwendel/spark), [`Flask`](https://github.com/pallets/flask),
    [`Spark`](https://github.com/perwendel/spark) to implement your front-end service. You can
    also reuse the code you wrote in lab 2 if you prefer.

3.  Like in lab 2, you can decide the interfaces used between the microservices. Each microservice
    still need to be able to handle requests concurrently.

4.  Add some variety to the toy offerings. Initialize your catalog with at least 20 different toys.
    You can consider add some toys from the [National Toy Hall of
    Fame](https://en.wikipedia.org/wiki/National_Toy_Hall_of_Fame). Also the catalog service will
    periodically restock the toys that are out of stock. The catalog service should check remaining
    quantity of every toy, if a toy is out of stock the catalog service will restock it.

## Part 1: Caching
