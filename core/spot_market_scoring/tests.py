from core.spot_market_scoring.user import get_client_list

# random forest test
# d_path = os.path.join(conf.ROOT_PATH, 'data', 'samples')
#
# params = 'us-east-1b', 'Linux/UNIX (Amazon VPC)', 'r5.xlarge', 2018
#
# a_df = a_df.drop_duplicates(subset=['Timestamp']).reset_index(drop=True)
# a_df['Timestamp'] = a_df['Timestamp'].apply(lambda x: Dtm(x).to_utm().to_int())
#
# plt.plot(dss.index, dss.values)
# plt.xticks(rotation=45)
# # plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(50))
# plt.show()
#
# for i_df in (a_df,b_df):
#     plt.plot(i_df['Timestamp'], i_df['Price'])
#     plt.xticks(rotation=45)
#     plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(50))
#     plt.gca().xaxis.set_major_locator(mdates.HourLocator(byhour=[0,1]))
#     plt.show()
#     train_spot_price_prediction_model(i_df)
#
#
# # df.set_index('Timestamp', inplace=False)
# i_df = df[['Timestamp', 'us-east-1a']]
# i_df_index = i_df.set_index('Timestamp')
# values = i_df_index.values
#
# rfs = train_spot_price_prediction_model(i_df_index)
# predict(values, rfs[-1])

def test_users():
    import boto3
    from spot_market_scoring import users
    ec2 = boto3.client('ec2', **settings.AWS_CREDENTIALS)

    # Retrieves all regions/endpoints that work with EC2
    regions = users.get_region_list(ec2)
    print('Regions:', regions)

    # Retrieves availability zones only for region of the ec2 object
    az = users.get_availability_zones(ec2)
    print('Availability Zones:', az)
