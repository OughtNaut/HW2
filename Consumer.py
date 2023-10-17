import argparse
import boto3

def consume():
    parser = argparse.ArgumentParser(
        prog='Consumer.py',
        description='Consumes Widget requests from given s3 bucket',
        epilog=''
    )
    parser.add_argument('-rb', '--read_bucket')
    parser.add_argument('-wb', '--write_bucket')
    parser.add_argument('-wt', '--write_table')

    args = parser.parse_args()

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(
        Bucket=args.read_bucket,
        MaxKeys=1
    )
    print(response.get('Contents')[0].get('Key'))

if __name__ == '__main__':
    consume()
