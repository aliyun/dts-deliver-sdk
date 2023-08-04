package com.aliyun.dts.deliver.commons.openapi;

import com.aliyun.dts20200101.models.DescribeChannelAccountRequest;
import com.aliyun.dts20200101.models.DescribeChannelAccountResponse;
import com.aliyun.dts20200101.models.DescribeChannelAccountResponseBody;
import com.aliyun.tea.TeaException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DtsOpenApi {
    private static final Logger LOG = LoggerFactory.getLogger(DtsOpenApi.class);

    private String ak;
    private String secret;
    private String dtsInstance;
    private String region;

    private String userName;
    private String password;

    public DtsOpenApi(String ak, String secret, String dtsInstance, String region) {
        this.ak = ak;
        this.secret = secret;
        this.dtsInstance = dtsInstance;
        this.region = region;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }


    public Pair<String, String> getUserPassword() throws Exception {

        if (StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(password)) {
            return Pair.of(userName, password);
        }

        com.aliyun.dts20200101.Client client  = createClient(ak, secret);

        DescribeChannelAccountRequest describeChannelAccountRequest = new DescribeChannelAccountRequest();
        describeChannelAccountRequest.setDtsJobId(dtsInstance);
        describeChannelAccountRequest.setRegionId(region);

        com.aliyun.teautil.models.RuntimeOptions runtime = new com.aliyun.teautil.models.RuntimeOptions();

        Exception e = null;
        try {
            // 复制代码运行请自行打印 API 的返回值
            DescribeChannelAccountResponse describeChannelAccountResponse =
                    client.describeChannelAccountWithOptions(describeChannelAccountRequest, runtime);

            Integer statusCode = describeChannelAccountResponse.getStatusCode();
            LOG.info("statusCode: " + statusCode);

            DescribeChannelAccountResponseBody describeChannelAccountResponseBody = describeChannelAccountResponse.getBody();
            String userName = describeChannelAccountResponseBody.getUsername();
            String password = describeChannelAccountResponseBody.getPassword();
            LOG.info("user: " + userName + ", password: " + password);

            return Pair.of(userName, password);

        } catch (TeaException error) {
            // 如有需要，请打印 error
            LOG.error(error.message);
            e = error;
        } catch (Exception _error) {
            TeaException error = new TeaException(_error.getMessage(), _error);
            // 如有需要，请打印 error
            LOG.error(error.message);
            e = error;
        }

        throw new RuntimeException(e);
    }

    //todo(yanmen)
    public String getDblist() {
        return "{\"dts_deliver_test\":{\"name\":\"dts_deliver_test\",\"all\":false,\"Table\":{\"tab1\":{\"name\":\"tab1\",\"all\":true},\"tab2\":{\"name\":\"tab2\",\"all\":true}}}}";
    }

    public com.aliyun.dts20200101.Client createClient(String accessKeyId, String accessKeySecret) throws Exception {
        com.aliyun.teaopenapi.models.Config config = new com.aliyun.teaopenapi.models.Config()
                // 必填，您的 AccessKey ID
                .setAccessKeyId(accessKeyId)
                // 必填，您的 AccessKey Secret
                .setAccessKeySecret(accessKeySecret);

        config.setRegionId(region);

        // 访问的域名
        return new com.aliyun.dts20200101.Client(config);
    }
}
