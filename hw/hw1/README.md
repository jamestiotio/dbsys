# SUTD 2021 50.043 Homework Assignment 1

> James Raphael Tiovalen / 1004555

## Setup Instructions

> These setup instructions assume that we are using Ubuntu 18.04.

The CSV files are put by default in the `/tmp/sql` folder or directory. If errors are encountered, you can attempt these possible different fixing options:

> You might need to combine some or even all of these fixes, or in some cases, perhaps you don't even need any of them at all. YMMV, depending on your current environment setup.

1. Add the line `secure-file-priv = "/tmp/sql/"` (using either hyphens/underscores for the variable name, with/without the single/double quotation marks or/and the trailing slash, with/without the whitespaces in-between) to `/etc/mysql/my.cnf` under the `[mysqld]` section (add one if it does not exist yet) and restart the `mysql` service (`sudo service mysql restart` or `/etc/init.d/mysql restart`, or alternatively by first running `sudo service mysql stop` and then running `sudo service mysql start`). The lines can be appended by using a text editor application available in your system, such as `nano` (`sudo nano <path_to_file_from_current_directory>`). Other files that you might also want/need to modify include: `/etc/my.cnf`, `~/.my.cnf`, `/etc/mysql/mysql.conf.d/mysqld.cnf`, or/and `/usr/local/etc/my.cnf`. You can find the default files inspected/parsed/read by MySQL (as well as the file-reading priority order) by running `mysql --help`. You can ensure that the value of `secure_file_priv` has been changed by running either `sudo mysql -u root -e 'SELECT @@secure_file_priv'` or `sudo mysql -u root -e 'SHOW VARIABLES LIKE "secure_file_priv"'`. Make sure that the `/tmp/sql/` directory already exists before adding the line and restarting the service, or else MySQL will fail to start.

2. Give world-readable permissions to the CSV files (run `sudo chmod 755 .` and `sudo chmod 744 *.csv` from the `/tmp/sql` directory).

3. Give ownership over the CSV files to `mysql` (run `sudo chown mysql .` and `sudo chown mysql *.csv` from the `/tmp/sql` directory).

4. Grant `FILE` permissions to the designated MySQL user (`sudo mysql -u root -e 'GRANT FILE ON *.* TO "root"@"%"'`).

5. Put the CSV files in the default MySQL folder (`/var/lib/mysql-files`) and load them from there instead. This requires editing the SQL submission file and it might not work in a different pre-determined environment (such as one that is configured to load said CSV files from `/tmp/sql`), and hence it is not recommended.

6. _(UNSAFE!!!)_ Disable the `--secure-file-priv` option in MySQL entirely (by setting `secure_file_priv = ""`). Only use this as a last-ditch resort!

## Usage

Then, to run the SQL file, simply execute `mysql -u root < hw1.sql`.

If your MySQL environment requires a password, simply execute `mysql -u root -p < hw1.sql` instead and specify your password in the following prompted line in the console.
