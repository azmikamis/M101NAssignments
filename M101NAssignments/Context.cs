using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Assignments;

namespace m101n
{
    public class Context
    {
        private IAssignment assignment;

        public Context(IAssignment assignment)
        {
            this.assignment = assignment;
        }

        public void doAssignment()
        {
            this.assignment.doAssignment();
        }
    }
}
