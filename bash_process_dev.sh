#!/bin/bash

# find where this script is
pushd `dirname $0` > /dev/null
FEED_PROCESSOR=`pwd -P`
popd > /dev/null

CRON_LOG=$FEED_PROCESSOR/logs/cron.log

cd $FEED_PROCESSOR
echo "" >> $CRON_LOG 2>&1
echo "" >> $CRON_LOG 2>&1
echo "$(date) Starting run... " >> $CRON_LOG 2>&1
python cw_shownotifications_feedprocessor.py -p -g -e dev -v >> $CRON_LOG 2>&1
echo "$(date) Finished." >> $CRON_LOG 2>&1