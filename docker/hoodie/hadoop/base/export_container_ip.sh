interfaces=( "en0" "eth0" )

ipAddr=""
for interface in "${interfaces[@]}"
do
  ipAddr=`ifconfig $interface | grep -Eo 'inet (addr:)?([0-9]+\.){3}[0-9]+' | grep -Eo '([0-9]+\.){3}[0-9]+' | grep -v '127.0.0.1' | head`
  if [ -n "$ipAddr" ]; then
    break
  fi 
done

echo "Container IP is set to : $ipAddr"
export MY_CONTAINER_IP=$ipAddr
