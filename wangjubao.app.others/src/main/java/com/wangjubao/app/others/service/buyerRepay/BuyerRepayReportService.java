package com.wangjubao.app.others.service.buyerRepay;

import java.text.ParseException;
import java.util.Date;

public interface BuyerRepayReportService {

    void anaBuyerRepayReport(Long sellerId, Date startDate) throws ParseException;
}
