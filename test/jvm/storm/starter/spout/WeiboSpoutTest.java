package storm.starter.spout;

import junit.framework.Assert;

import org.junit.Test;



public class WeiboSpoutTest {
	@Test
	public void testExtractTopic() {
		String weibo = "这是一个#话题#";
		WeiboSpout spout = new WeiboSpout();
	    Assert.assertEquals(spout.extractTopic(weibo), "话题");	
	    
	    weibo = "这里没有话题";
	    Assert.assertNull(spout.extractTopic(weibo));
	}

	@Test
	public void testLog() {
		WeiboSpout.LOG.info("this is log");
	}
}
