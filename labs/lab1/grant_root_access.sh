#!/bin/bash

sudo mysql -e 'UPDATE mysql.user SET plugin = "mysql_native_password" WHERE user = "root"'
sudo mysql -e 'CREATE user "root"@"%" IDENTIFIED BY ""'
sudo mysql -e 'GRANT ALL PRIVILEGES ON *.* TO "root"@"%" WITH GRANT OPTION'
sudo mysql -e 'FLUSH PRIVILEGES'
sudo service mysql restart
