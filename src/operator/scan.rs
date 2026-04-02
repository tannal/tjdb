use crate::storage::Tuple;
use crate::operator::{Operator, ExecuteResult};

pub struct ScanOperator<'a> {
    // 使用 std::slice::Iter 来遍历存储在 Table 中的数据
    data_iter: std::slice::Iter<'a, Tuple>,
}

impl<'a> ScanOperator<'a> {
    pub fn new(data: &'a Vec<Tuple>) -> Self {
        Self {
            data_iter: data.iter(),
        }
    }
}

impl<'a> Iterator for ScanOperator<'a> {
    type Item = ExecuteResult;

    fn next(&mut self) -> Option<Self::Item> {
        // 将引用转化为克隆的 Tuple 以供流水线向上层传递
        self.data_iter.next().cloned().map(Ok)
    }
}

// 标记为 Operator 满足 Trait 约束
impl<'a> Operator for ScanOperator<'a> {}