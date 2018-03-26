# AWS lambda function - data-staged

## adding external libraries
```
cd hysds-cloud-functions/aws/data-staged
pip install -t <lib> .
```

For example to install `requests`
```
cd hysds-cloud-functions/aws/data-staged
pip install -t requests .
```

## create deployment package
```
cd hysds-cloud-functions/aws/data-staged
zip -r -9 ../data-staged.zip *
```
