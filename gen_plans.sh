#make command file

rm -rf /tmp/results/*

(
echo "\f raw" 
echo "\e"
for q in $(ls /scratch/TPCH/{01..22}.sql);
do
    echo "\> /tmp/results/$(basename $q).mplan"
    echo "$(cat $q | grep -v 'default substitutions' | grep -v '^$' | grep -v "optimizer_stats()" | sed 's/^select/plan select/g')"
done
) > /tmp/mclient_plan_script.sql
                                                                             
mclient -i tpch001 /tmp/mclient_plan_script.sql
