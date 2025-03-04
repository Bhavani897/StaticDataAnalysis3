package com.rite.products.convertrite.service;

import java.util.List;
import com.rite.products.convertrite.model.CrObjectGroupLines;
import com.rite.products.convertrite.model.CrObjectGroupLinesView;
import com.rite.products.convertrite.po.BasicResPo;
import com.rite.products.convertrite.po.CrObjectGroupLinesReqPo;

public interface CrObjectGroupLinesService {

	public List<CrObjectGroupLines> getAllByGroupId(Long groupId);
	
    public List<CrObjectGroupLines>	 saveAllObjectGroupLinesByGroupId(List<CrObjectGroupLinesReqPo> lineList, Long groupId); // Returns Table Entity List
    
    public String deleteAllLinesByGroupId(Long groupId);
    
    public List<CrObjectGroupLinesView>	 saveAllObjectGrpLinesByGroupId(List<CrObjectGroupLinesReqPo> lineList, Long groupId); // Returns View Entity List
    
    public List<CrObjectGroupLinesView> getAllLinesViewByGroupId(Long groupId);

   public BasicResPo deleteLinesByobjGrpLineId(Long objGrpLineId);

    BasicResPo getUngroupedObjects(Long projectId, String objectCode);
}
