# sudo service mysql restart --use in case mysql doesnt load
rm etl_state.json
mysql -u azalia2 -p123456 < flights_db.sql &&
mysql -u azalia2 -p123456 < flights_views.sql &&
source venv/bin/activate &&
python run_etl.py --sample-size 10000




# python -m debugpy --listen 0.0.0.0:5461 --wait-for-client run_etl.py  --sample-size 1001 --reset-position --start-row 5800000
