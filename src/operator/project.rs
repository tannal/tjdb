use crate::storage::Tuple;
use crate::operator::{Operator, ExecuteResult};

pub struct ProjectOperator<'a> {
    // 上层持有下层算子的所有权
    source: Box<dyn Operator + 'a>,
    // 预先计算好的列索引，例如 [0, 2] 代表只取第0列和第2列
    col_indices: Vec<usize>,
}

impl<'a> ProjectOperator<'a> {
    pub fn new(source: Box<dyn Operator + 'a>, col_indices: Vec<usize>) -> Self {
        Self {
            source,
            col_indices,
        }
    }
}

impl<'a> Iterator for ProjectOperator<'a> {
    type Item = ExecuteResult;

    fn next(&mut self) -> Option<Self::Item> {
        // 调用下层算子的 next()
        self.source.next().map(|item| {
            item.map(|tuple| {
                // 根据索引重新构造 Tuple
                let projected_data = self.col_indices
                    .iter()
                    .map(|&idx| tuple.0[idx].clone())
                    .collect();
                Tuple(projected_data)
            })
        })
    }
}

impl<'a> Operator for ProjectOperator<'a> {}