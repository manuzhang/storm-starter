package storm.starter.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import weibo4j.Timeline;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;
import weibo4j.model.WeiboException;
import weibo4j.util.WeiboConfig;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WeiboSpout extends BaseRichSpout {

	private static final long serialVersionUID = -842703683486344849L;
	private SpoutOutputCollector weiboCollector;
	private Timeline timeline;

	public static final long UPDATE_FREQUENCY = 30000; // update every 30 secs

	public static final Logger LOG = Logger.getLogger(WeiboSpout.class);

	public WeiboSpout() {

	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		weiboCollector = collector;
		String access_token = WeiboConfig.getValue("access_token");
		timeline = new Timeline();
		timeline.client.setToken(access_token);	
	}

	@Override
	public void nextTuple() {
		try {
			StatusWapper wrapper = timeline.getPublicTimeline();
			List<String> topicList = new ArrayList<String>();
			for (Status status : wrapper.getStatuses()) {
				String text = status.getText();
				String topic = extractTopic(text);
				if (topic != null) {
					LOG.info("topic: " + topic);
					topicList.add(topic);
				}
			}
			weiboCollector.emit(new Values(topicList));
		} catch (WeiboException e) {
			LOG.error(e.getMessage());
		}
		Utils.sleep(UPDATE_FREQUENCY);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}  

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("weibo"));
	}

	protected String extractTopic(String text) {
		int start = text.indexOf("#");
		String topic = null;
		if (start != -1) {
			int end = text.indexOf("#", start + 1);
			if (end != -1) {
				topic = text.substring(start + 1, end);
			}
		}
		return topic;
	}

}
