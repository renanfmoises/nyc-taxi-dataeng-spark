
set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in {10..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  FULL_URL="${BASE_URL}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

  BASE_LOCAL_PATH="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
  FULL_LOCAL_PATH="${BASE_LOCAL_PATH}/${LOCAL_FILE}"

  echo "Downloading ${FULL_URL} to ${FULL_LOCAL_PATH}"
  mkdir -p ${BASE_LOCAL_PATH}
  curl -sSL ${FULL_URL} > ${FULL_LOCAL_PATH}

  # echo "Compressing ${FULL_LOCAL_PATH}"
  # gzip ${FULL_LOCAL_PATH}
done
