package com.marklogic.developer.corb;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.marklogic.developer.corb.TopUriList.UriObject;

class TopUriList{
	class UriObject implements Comparable<UriObject>{
		@Override
		public String toString() {
			return "UriObject [uri=" + uri + ", timeTaken=" + timeTaken + "]";
		}
		String uri;
		Long timeTaken;
		public UriObject(String uri, Long timeTaken) {
			super();
			this.uri = uri;
			this.timeTaken = timeTaken;
		}
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof UriObject){
				UriObject o = (UriObject)obj;
				if(this.timeTaken!=null && o.timeTaken!=null) {
					return this.timeTaken.compareTo(o.timeTaken) == 0;
				}
				else{
					return false;
				}
			}
			else return super.equals(obj);
		}
		@Override
		public int compareTo(UriObject o) {
			if(this.timeTaken!=null && o.timeTaken!=null) {
				return this.timeTaken.compareTo(o.timeTaken);
			}
			else{
				return 0;//should never get here
			}
		}
	}
	TreeSet<UriObject>  list = null;

	public TopUriList(int size) {
		this.size = size;
		list = new TreeSet<UriObject>() {
			private static final long serialVersionUID = 1L;
			public String toString() {
				StringBuffer strBuff = new StringBuffer();
				for (UriObject o : this) {
					strBuff.append(o.toString());
				}
				return strBuff.toString();
			}
		};
	}
	int size=0;
	Map<String,Long> getData(){
		Map<String,Long> map = new HashMap<String,Long>();
		for(UriObject obj:this.list){
			map.put(obj.uri, obj.timeTaken);
		}
		return map;
	}
	void add(String uri, Long timeTaken){
		UriObject newObj=new UriObject(uri, timeTaken);
		if(list.size()<this.size || list.last().compareTo(newObj) <1){
			synchronized (list) {
				if(list.size()>=this.size ){
					for(int i=0; i<=list.size()-this.size; i++){
						list.remove(list.first());						
					}
				}
				list.add(newObj);
			}
		}
	}
}