curl -XPUT 'http://localhost:9200/clang' -H 'Content-Type: application/json' -d '
{
"settings" : {
"number_of_shards" : 1,
"number_of_replicas" : 1
}
}'