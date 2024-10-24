#!/bin/bash

echo "============> 模拟数据 <============"
ssh hadoop102 "cd /opt/module/fast_food; java -jar mock-fastfood-1.0.1.jar"

