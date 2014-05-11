for i in {1..5}
do
	python updateinfo.py douban$i.csv &
	echo "save file in douban$i.csv"
done
