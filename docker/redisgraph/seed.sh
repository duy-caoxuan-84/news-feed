#!/bin/sh
echo "Seeding RedisGraph social_graph..."
cat /scripts/init.cypher | redis-cli
echo "RedisGraph seeding completed."
