#!/bin/sh

branch=master
appName=wallet
if [ ! -d "/opt/seamless_wallet/" ]; then
     git clone git@ip:demo/seamless_wallet.git
fi
cd /opt/seamless_wallet/
git reset --hard origin/${branch}
git pull
echo -e "拉取最新代码成功"
go build -o ../wallet
echo -e "项目 [wallet] 编译成功"

# 应用程序数组
apps=("wallet8081" "wallet8082")

# 循环处理每个服务
for service in "${apps[@]}"; do
    portDir="${service//[^0-9]/}"
    echo "服务名称： $service"
    while true; do
        processNum=$(ps -ef | grep $service | grep -v grep | wc -l)
        if [ $processNum -eq 1 ]; then
            echo -e "开始执行终止 [$service] 进程"
            ps -aux | grep $service | grep -v grep | awk '{print $2}' | xargs kill
            sleep 1
        else
            echo -e "$service 进程已经终止, $service 停止服务"
            break
        fi
        sleep 1
    done

    # 检查目录是否存在，不存在则创建
    if [ ! -d "/opt/$portDir/" ]; then
        mkdir "/opt/$portDir/"
        echo -e "创建目录 /opt/$portDir/"
    fi

    # 复制最新启动包
    service=$(echo "$service" | tr -d '[:space:]')
    cp "/opt/$appName" "/opt/$portDir/$service"
    chmod 755 "/opt/$portDir/$service"
    echo -e "$service 最新启动包复制成功"

    while true; do
        processNum=$(ps -ef | grep $service | grep -v grep | wc -l)
        if [ $processNum -eq 0 ]; then
           echo -e "/opt/$portDir/$service > /dev/null 2>&1 &"
           # shellcheck disable=SC2164
           cd "/opt/$portDir/"
           nohup ./$service > /dev/null 2>&1 &
           echo -e "command executed successfully."
           sleep 1
        else
          serviceInfo=$(ps -ef | grep "$service | grep -v grep")
          echo -e "service-info[$service]: \r\r$serviceInfo\r"
          echo -e "start $service success. 服务已启动, 执行后续操作"
          # 查看日志命令
          echo -e "tail -f /opt/$portDir/logs/$(date +%Y-%m-%d).log -n 50"
          echo -e "------------------ 进入15秒等待状态... ------------------"
          sleep 15
          break
        fi
    done
done
serviceInfo=$(ps -ef | grep "$appName" | grep -v grep)
echo -e "\r\r------------------> 服务已全部启动 <------------------"
echo -e "service-info[$appName]: \r\r$serviceInfo\r\r"
