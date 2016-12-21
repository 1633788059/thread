package com.wangjubao.app.others.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(locations = { "classpath*:/META-INF/spring/*.xml", "classpath*:/spring/*.xml" })
public abstract class AbstractWangjubaoTest extends AbstractJUnit4SpringContextTests {
    protected static Logger          logger = LoggerFactory.getLogger(AbstractWangjubaoTest.class);

    @Autowired
    protected ReadHistoryDataService readHistoryDataService;
}
