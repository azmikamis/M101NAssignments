using System;
using Assignments;

namespace m101n
{
    class Program
    {
        static void Main(string[] args)
        {
            Context context = new Context(new Week54Assignment());
            context.doAssignment();
            Console.WriteLine("Press Enter");
            Console.ReadLine();
        }
    }
}
