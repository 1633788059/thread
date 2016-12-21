package com.wangjubao.app.others.service.impl;

import java.io.File;
import java.io.FileFilter;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.wangjubao.app.others.service.IHisTradeCsvImportService;
import com.wangjubao.app.others.service.IHistoryTradeImportService;
import com.wangjubao.app.others.service.ITaobaoHistoryTradeCsvImportService;
import com.wangjubao.core.domain.syn.HisImport;
import com.wangjubao.core.service.basic.ICommonService;
import com.wangjubao.core.service.job.OtherJobServer;

public class HisTradeCsvImportServiceImpl implements IHisTradeCsvImportService {
	protected transient final static Logger logger = Logger.getLogger("others");

	private ITaobaoHistoryTradeCsvImportService taobaoHistoryTradeCsvImportService;
	private ICommonService commonService;
	private OtherJobServer otherJobService;

	protected String buildFilePath(long sellerId, String basePath) {
		return basePath + File.separator + sellerId + File.separator;

	}

	private File[] readFileList(Long sellerId, String basePath) {
		String path = this.buildFilePath(sellerId, basePath);
		File folder = new File(path);
		if (!folder.isDirectory() || !folder.exists()) {
			logger.info(path + "目录不存在");
			throw new RuntimeException(path + "目录不存在");
		}

		return folder.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				if (pathname.getAbsolutePath().endsWith("csv") || pathname.getAbsolutePath().endsWith("CSV"))
					return true;
				return false;
			}

		});
	}

	public void jobHisImportTrade(Long sellerId, String basePath) {
		final File[] files = this.readFileList(sellerId, basePath);
		if (files == null || files.length < 1)
			throw new RuntimeException("目录不存在或没有要处理的文件");
		final StringBuilder sb = new StringBuilder();
		String str[] = new String[files.length];
		int index = 0;
		for (File file : files) {
			sb.append(file.getName()).append(";");
			str[index] = file.getName();
			index++;
		}
		HisImport im = new HisImport();
		im.setCreated(new Date());
		im.setSellerId(sellerId);
		im.setStatus(HisImport.STATUS_1);
		im.setFileStr(sb.toString());
		im.setImpDate(new Date());
		commonService.insertHisImport(im);
		final Long id = im.getId();
		taobaoHistoryTradeCsvImportService.importTrade(files, sellerId, basePath, id);
	}

	@Override
	public synchronized String[] importHisTrade(final Long sellerId, final String platform, final String basePath) {
		HisImport query = new HisImport();
		query.setSellerId(sellerId);
		List<HisImport> list = this.commonService.queryHisImportList(query);
		for (HisImport imp : list) {
			if (imp.getStatus().intValue() != HisImport.STATUS_7 && imp.getStatus().intValue() != HisImport.STATUS_6) {
				throw new RuntimeException("此卖家还有未执行完的任务导入");
			}
		}

		final File[] files = this.readFileList(sellerId, basePath);
		if (files == null || files.length < 1)
			throw new RuntimeException("目录不存在或没有要处理的文件");
		// final StringBuilder sb = new StringBuilder();
		String str[] = new String[files.length];
		int index = 0;
		for (File file : files) {
			// sb.append(file.getName()).append(";");
			str[index] = file.getName();
			index++;
		}
		// 创建job
		try {
			otherJobService.saveHisTradeImpJob(sellerId, basePath);
		} catch (Exception ex) {
			throw new RuntimeException("", ex);
		}
		return str;
	}

	
	public ITaobaoHistoryTradeCsvImportService getTaobaoHistoryTradeCsvImportService() {
		return taobaoHistoryTradeCsvImportService;
	}

	public void setTaobaoHistoryTradeCsvImportService(
			ITaobaoHistoryTradeCsvImportService taobaoHistoryTradeCsvImportService) {
		this.taobaoHistoryTradeCsvImportService = taobaoHistoryTradeCsvImportService;
	}

	public ICommonService getCommonService() {
		return commonService;
	}

	public void setCommonService(ICommonService commonService) {
		this.commonService = commonService;
	}

	public OtherJobServer getOtherJobService() {
		return otherJobService;
	}

	public void setOtherJobService(OtherJobServer otherJobService) {
		this.otherJobService = otherJobService;
	}

}
