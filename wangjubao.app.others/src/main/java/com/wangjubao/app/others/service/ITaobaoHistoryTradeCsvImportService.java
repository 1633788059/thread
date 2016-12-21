package com.wangjubao.app.others.service;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.csvreader.CsvReader;
import com.wangjubao.core.dao.IAreaDao;
import com.wangjubao.core.domain.buyer.SellerUser;
import com.wangjubao.core.domain.seller.TaobaoOrder;
import com.wangjubao.core.domain.seller.TaobaoTrade;
import com.wangjubao.core.domain.seller.TaobaoUser;
import com.wangjubao.core.domain.syn.HisImport;
import com.wangjubao.core.domain.syn.HisImportProcess;
import com.wangjubao.core.service.basic.ICommonService;
import com.wangjubao.core.service.buyer.BuyerAddupVO;
import com.wangjubao.core.service.buyer.IBuyerService;
import com.wangjubao.core.service.seller.ITaobaoService;
import com.wangjubao.framework.util.DateUtil;

public interface ITaobaoHistoryTradeCsvImportService  {

	public String importTrade(long sellerId, String basePath);

	public String importTrade(File[] files, Long sellerId, String basePath, Long id) ;


}
