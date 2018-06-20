package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
*类名和方法不能修改
 */
public class Schedule {
	private List<Integer> nodes = new ArrayList<Integer>();
	private List<Integer> tasks = new ArrayList<Integer>();

	private Map<Integer, Integer> taskMap = new HashMap<Integer, Integer>();
	
	private Map<Integer,List<TaskInfo>> taskStatus = new HashMap<Integer, List<TaskInfo>>(); 

	Comparator<TaskInfo> compareByTaskId = new Comparator<TaskInfo>() {
		public int compare(TaskInfo o1, TaskInfo o2) {
			return (o1.getTaskId() - o2.getTaskId());
		}
	};

	Comparator<Integer> compareByTime = new Comparator<Integer>() {
		public int compare(Integer o1, Integer o2) {
			return (taskMap.get(o2) - taskMap.get(o1));
		}
	};

	/**
	 * 系统初始化
	 * @return
	 */
	public int init() {
		
		return ReturnCodeKeys.E001;

	}

	/**
	 * 服务节点注册
	 * @param nodeId
	 * @return
	 */
	public int registerNode(int nodeId) {
		
		if (nodeId <= 0) 
			return ReturnCodeKeys.E004;
		
		if (nodes.contains(nodeId)) 
			return ReturnCodeKeys.E005;
		
		nodes.add(nodeId);
		Collections.sort(nodes);
		return ReturnCodeKeys.E003;

	}

	/**
	 * 服务节点注销
	 * @param nodeId
	 * @return
	 */
	public int unregisterNode(int nodeId) {

		if (nodeId <= 0) 
			return ReturnCodeKeys.E004;
		
		if (!nodes.contains(nodeId))
			return ReturnCodeKeys.E007;

		nodes.remove(new Integer(nodeId));
		return ReturnCodeKeys.E006;
	}

	/**
	 * 添加任务
	 * @param taskId
	 * @param consumption
	 * @return
	 */
	public int addTask(int taskId, int consumption) {

		if (taskId <= 0)
			return ReturnCodeKeys.E009;

		if (tasks.contains(taskId))
			return ReturnCodeKeys.E010;

		tasks.add(taskId);
		taskMap.put(taskId, consumption);
		Collections.sort(tasks, compareByTime);
		return ReturnCodeKeys.E008;
	}

	/**
	 * 删除任务
	 * @param taskId
	 * @return
	 */
	public int deleteTask(int taskId) {

		if(taskId<=0)
			return ReturnCodeKeys.E009;
		
		if (!tasks.contains(taskId))
			return ReturnCodeKeys.E012;

		tasks.remove(new Integer(taskId));
		taskMap.remove(new Integer(taskId));
		return ReturnCodeKeys.E011;
	}
	
	/**
	 * 任务调度
	 * @param threshold 调度阀值
	 * @return
	 */
	public int scheduleTask(int threshold) {

		//调度阀值非法
		if(threshold<=0)
			return ReturnCodeKeys.E002;
		
		if (tasks.isEmpty())
			return ReturnCodeKeys.E014;
		
		else{
			//任务调度成功,代码未完成
			return ReturnCodeKeys.E013;
		}
	}

	/**
	 * 查询任务状态列表
	 * @param tasks
	 * @return
	 */
	public int queryTaskStatus(List<TaskInfo> tasks) {

		for (Integer nodeId : taskStatus.keySet()) {
			tasks.addAll(taskStatus.get(nodeId));
		}

		Collections.sort(tasks, compareByTaskId);
		return ReturnCodeKeys.E015;
	}
}
